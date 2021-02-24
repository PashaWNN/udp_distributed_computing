import threading
import time
import tkinter as tk
from tkinter import messagebox
from typing import Callable

from computing import DistributionComputingClient
from udp import UDPClient, command, GET_CHUNK, RESULT_PART, TASK, ACKNOWLEDGE, NO_JOB, MATH_ERROR
from utils import validate_float, tk_validator

default_server_address = '127.0.0.1'
default_server_port = 20001


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


class MainWindow:
    def __init__(self, r):
        self.root = r
        self.server_ip = tk.Entry(r, width=20)
        self.server_ip.insert(0, default_server_address)
        self.server_port = tk.Entry(r, width=20, validate='key', validatecommand=tk_validator(r, validate_float))
        self.server_port.insert(0, str(default_server_port))

        grid = [
            [tk.Label(r, text='IP сервера'), self.server_ip],
            [tk.Label(r, text='Порт сервера'), self.server_port],
            [tk.Button(r, text='Запуск', command=self.work)]
        ]
        for i, row in enumerate(grid):
            for j, col in enumerate(row):
                col.grid(row=i, column=j)
        self.log_text = None
        self.thread = None
        self.root.protocol("WM_DELETE_WINDOW", self.stop_thread)

    def log(self, text):
        if self.log_text is None:
            return
        self.log_text.insert(tk.END, text + '\n')

    def prepare_console(self):
        for item in self.root.winfo_children():
            item.destroy()
        self.log_text = tk.Text(self.root, width=100)
        self.log_text.grid(row=0, column=0)

    def stop_thread(self):
        self.thread.do_work = False
        exit()

    def work(self):
        server_ip = self.server_ip.get()
        server_port = int(self.server_port.get())
        try:
            controller = DistributionComputingClient()
            client = ClusterClient(server_ip, server_port, controller, logging_callback=self.log)
            self.thread = threading.Thread(target=client.work)
            self.thread.do_work = True
            self.thread.start()
        except Exception as e:
            messagebox.showwarning('Ошибка при запуске клиента', str(e))
            return
        self.prepare_console()


def main():
    root = tk.Tk()
    root.title('Клиент')
    MainWindow(root)
    root.mainloop()


if __name__ == '__main__':
    main()
