import threading
import time
import tkinter as tk
from tkinter import messagebox
from typing import Callable

from computing import DistributionComputingClient
from gui import Application, LoggerWindow, Button, Window, Entry
from udp import UDPClient, command, GET_CHUNK, RESULT_PART, TASK, ACKNOWLEDGE, NO_JOB, MATH_ERROR
from utils import validate_float, tk_validator, validate_port, validate_ip

default_server_ip = '127.0.0.1'
default_server_port = '20001'


class ClusterClient(UDPClient):
    """
    Класс, определяющий логику работы клиента кластерных вычислений
    """

    def __init__(self,
                 ip_address: str,
                 port: int,
                 controller: DistributionComputingClient,
                 logging_callback: Callable = None
                 ):
        super().__init__(ip_address, port)
        self.working = False
        self.bounds = (0, 0)
        self.controller = controller
        self.print = logging_callback or print

    def work(self):
        """
        Переопределённый метод основного цикла.
        Попросить работу, а затем выполнять основной цикл как задано в родительском классе
        """
        self.send(GET_CHUNK)
        super().work()

    def on_timeout(self):
        """
        В случае, если долго ничего не приходит, попросить работу
        """
        self.send(GET_CHUNK)

    def pre_loop(self):
        do_work = getattr(threading.currentThread(), 'do_work', True)
        if not do_work:
            self.finished = True

    @command(NO_JOB)
    def process_sleep(self):
        """
        Метод обработки команды NO_JOB.

        Ждать две секунды и запросить работу снова
        """
        self.print('Ожидание заданий...')
        time.sleep(2)
        self.send(GET_CHUNK)

    @command(ACKNOWLEDGE)
    def process_acknowledge(self):
        """
        Метод обработки команды ACKNOWLEDGE

        Попросить ещё работу
        """
        self.print('Сервер подтвердил получение результата')
        self.send(GET_CHUNK)

    @command(TASK)
    def process_task(self, formula: str, method_name: str, lower_bound: float, higher_bound: float):
        """
        Метод обработки команды BOUNDS

        Установка границ вычислений, вычисление и отправка результата
        """
        self.controller.set_task(method_name, formula, lower_bound, higher_bound)
        try:
            result = self.controller.compute()
            self.print(f'Частичный результат {result}')
            self.send(RESULT_PART, result)
        except ValueError as e:
            if str(e) == 'math domain error':
                self.send(MATH_ERROR)
            else:
                raise


class MainClientWindow(Window):

    server_ip = Entry('IP-адрес', default=default_server_ip)
    server_port = Entry('Порт', validator=validate_port, default=default_server_port)
    go = Button('Запуск сервера', 'launch_clicked')

    def __init__(self, app):
        super().__init__(app)

    def launch_clicked(self):
        try:
            self.validate()
            self.start_working_thread()
            self.app.set_window_contents(LoggerWindow)
        except Exception as e:
            messagebox.showwarning('Ошибка при запуске клиента', str(e))
            return

    def validate(self):
        if not validate_ip(self['server_ip'].get()):
            raise ValueError('Введён невалидный IP-адрес')

    def start_working_thread(self):
        server_ip = self['server_ip'].get()
        server_port = int(self['server_port'].get())
        controller = DistributionComputingClient()
        logging_callback = lambda l: self.app.emit_event('log', l)
        client = ClusterClient(server_ip, server_port, controller, logging_callback=logging_callback)
        self.app.thread = threading.Thread(target=client.work)
        self.app.thread.start()


if __name__ == '__main__':
    app = Application('Сервер', MainClientWindow)
    app.mainloop()
