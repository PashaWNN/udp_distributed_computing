import threading
from tkinter import messagebox
from typing import Optional, Tuple, Union, Callable

from computing import DistributedComputingController
from gui import Window, Entry, Combobox, Button, LoggerWindow, Application
from udp import UDPServer, command, GET_CHUNK, RESULT_PART, TASK, ACKNOWLEDGE, RESET_WATCHDOG, NO_JOB, Prefix, \
    MATH_ERROR
from utils import validate_float, validate_int, validate_port, validate_ip

default_server_ip = '127.0.0.1'
default_server_port = '20001'
default_formula = '2 * x + 1 / sqrt(x + 1 / 16)'
default_cluster_count = '5'
default_lower_bound = '0.0'
default_higher_bound = '2.0'

SIM_RULE = 'Правило Симпсона'
TRAP_RULE = 'Правило трапеций'
rules_mapping = {
    SIM_RULE: 'SIM',
    TRAP_RULE: 'TRA',
}


class ClusterServer(UDPServer):

    def __init__(
            self,
            bind_address: str,
            bind_port: int,
            controller: DistributedComputingController,
            logging_callback: Callable = None,
    ):
        super().__init__(bind_address, bind_port)
        self.controller = controller
        self.print = logging_callback or print

    def pre_loop(self):
        self.controller.deallocate_abandoned_chunks()
        if self.controller.finished or not getattr(threading.current_thread(), 'do_work', True):
            self.finished = True

    @command(GET_CHUNK)
    def process_get_chunk(self, client_id: str) -> Union[Prefix, Tuple[Prefix, int, int]]:
        chunk = self.controller.allocate_chunk(client_id)
        if not chunk:
            return NO_JOB
        self.print(f'{chunk} был назначен {client_id}')
        left_bound, right_bound = chunk.get_bounds()
        return TASK, *self.controller.cluster_args, left_bound, right_bound

    @command(RESET_WATCHDOG)
    def process_watchdog(self, client_id: str):
        self.controller.reset_watchdog(client_id)

    @command(MATH_ERROR)
    def process_error(self, client_id: str):
        self.print(f'Клиент {client_id} сообщил о математической ошибке при вычислениях')
        self.finished = True

    @command(RESULT_PART)
    def process_result(self, result: float, client_id: str) -> Optional[Prefix]:
        self.print(f'Получен частичный результат от {client_id}')
        result = self.controller.add_result_part(client_id, result)
        if result is not None:
            self.finished = True
            self.print(f'Получен результат: {result}')
        return ACKNOWLEDGE


class MainServerWindow(Window):
    server_ip = Entry('IP-адрес', default=default_server_ip)
    server_port = Entry('Порт', validator=validate_port, default=default_server_port)
    lower_bound = Entry('Нижний предел инт-ния', validator=validate_float, default=default_lower_bound)
    higher_bound = Entry('Верхний предел инт-ния', validator=validate_float, default=default_higher_bound)
    clusters = Entry('Макс. кол-во кластеров', validator=validate_int, default=default_cluster_count)
    formula = Entry('Формула', default=default_formula)
    rule = Combobox('Метод', values=[SIM_RULE, TRAP_RULE], default=SIM_RULE)
    go = Button('Запуск сервера', 'launch_clicked')

    def __init__(self, app):
        super().__init__(app)

    def launch_clicked(self):
        try:
            self.validate()
            self.start_working_thread()
            self.app.set_window_contents(LoggerWindow)
        except Exception as e:
            messagebox.showwarning('Ошибка при запуске сервера', str(e))
            return

    def validate(self):
        if float(self['lower_bound'].get()) >= float(self['higher_bound'].get()):
            raise ValueError('Нижняя граница должна быть меньше верхней')
        if int(self['clusters'].get()) <= 0:
            raise ValueError('Количество кластеров должно быть положительным числом')
        if not validate_ip(self['server_ip'].get()):
            raise ValueError('Введён невалидный IP-адрес')

    def start_working_thread(self):
        lower_bound = float(self['lower_bound'].get())
        higher_bound = float(self['higher_bound'].get())
        cluster_count = int(self['clusters'].get())
        formula_to_integrate = self['formula'].get()
        server_ip = self['server_ip'].get()
        server_port = int(self['server_port'].get())
        method = self['rule'].get()
        controller = DistributedComputingController(
            lower_bound, higher_bound,
            cluster_count=cluster_count,
            formula=formula_to_integrate,
            computation_method=rules_mapping[method],
        )
        logging_callback = lambda l: self.app.emit_event('log', l)
        server = ClusterServer(server_ip, server_port, controller, logging_callback=logging_callback)
        self.app.thread = threading.Thread(target=server.work)
        self.app.thread.start()


if __name__ == '__main__':
    app = Application('Сервер', MainServerWindow)
    app.mainloop()
