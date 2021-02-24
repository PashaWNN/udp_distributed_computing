import threading
import tkinter as tk
from tkinter import ttk, messagebox
from typing import Optional, Tuple, Union, Callable

from computing import DistributedComputingController
from udp import UDPServer, command, GET_CHUNK, RESULT_PART, TASK, ACKNOWLEDGE, RESET_WATCHDOG, NO_JOB, Prefix, \
    MATH_ERROR
from utils import validate_float, tk_validator, validate_int

default_server_ip = '127.0.0.1'
default_server_port = 20001
default_formula = '2 * x + 1 / sqrt(x + 1 / 16)'
default_cluster_count = 5
default_lower_bound = 0.0
default_higher_bound = 2.0

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


class MainWindow:
    def __init__(self, r):
        self.root = r
        self.server_ip = tk.Entry(r, width=20)
        self.server_ip.insert(0, default_server_ip)
        self.server_port = tk.Entry(r, width=20, validate='key', validatecommand=tk_validator(r, validate_float))
        self.server_port.insert(0, str(default_server_port))
        self.lb = tk.Entry(r, width=10, validate='key', validatecommand=tk_validator(r, validate_float))
        self.lb.insert(0, str(default_lower_bound))
        self.hb = tk.Entry(r, width=10, validate='key', validatecommand=tk_validator(r, validate_float))
        self.hb.insert(0, str(default_higher_bound))
        self.clusters = tk.Entry(r, width=20, validate='key', validatecommand=tk_validator(r, validate_int))
        self.clusters.insert(0, str(default_cluster_count))
        self.formula = tk.Entry(r, width=20)
        self.formula.insert(0, default_formula)
        self.rule = ttk.Combobox(r, values=[SIM_RULE, TRAP_RULE])
        self.rule.insert(0, SIM_RULE)
        self.log_text = None

        grid = [
            [tk.Label(r, text='IP сервера'), self.server_ip],
            [tk.Label(r, text='Порт сервера'), self.server_port],
            [tk.Label(r, text='Пределы интегрирования'), self.lb, self.hb],
            [tk.Label(r, text='Количество кластеров'), self.clusters],
            [tk.Label(r, text='Метод интегрирования'), self.rule],
            [tk.Label(r, text='Формула для интегрирования'), self.formula],
            [tk.Button(r, text='Запуск', command=self.work)]
        ]
        for i, row in enumerate(grid):
            for j, col in enumerate(row):
                col.grid(row=i, column=j)
        self.root.protocol("WM_DELETE_WINDOW", self.stop_thread)

    def prepare_console(self):
        for item in self.root.winfo_children():
            item.destroy()
        self.log_text = tk.Text(self.root, width=100)
        self.log_text.grid(row=0, column=0)

    def log(self, text):
        if self.log_text is None:
            return
        self.log_text.insert(tk.END, text + '\n')

    def stop_thread(self):
        self.thread.do_work = False
        exit()

    def work(self):
        lower_bound = float(self.lb.get())
        higher_bound = float(self.hb.get())
        cluster_count = int(self.clusters.get())
        formula_to_integrate = self.formula.get()
        server_ip = self.server_ip.get()
        server_port = int(self.server_port.get())
        method = self.rule.get()
        try:
            controller = DistributedComputingController(
                lower_bound, higher_bound,
                cluster_count=cluster_count,
                formula=formula_to_integrate,
                computation_method=rules_mapping[method],
            )
            server = ClusterServer(server_ip, server_port, controller, logging_callback=self.log)
            self.thread = threading.Thread(target=server.work)
            self.do_work = True
            self.thread.start()
        except Exception as e:
            messagebox.showwarning('Ошибка при запуске сервера', str(e))
            return
        self.prepare_console()


if __name__ == '__main__':
    root = tk.Tk()
    window = MainWindow(root)
    root.title('Сервер')
    root.mainloop()

