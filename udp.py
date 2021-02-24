import ast
import socket
from typing import NewType, Tuple
from uuid import uuid4

BUFFER_SIZE = 1024
CLIENT_TIMEOUT = 5

# Константы, определяющие командные префиксы
Prefix = NewType('Prefix', str)
GET_CHUNK = Prefix('GET')
RESULT_PART = Prefix('GOT')
TASK = Prefix('TAS')
ACKNOWLEDGE = Prefix('ACK')
RESET_WATCHDOG = Prefix('DOG')
NO_JOB = Prefix('NOJ')
MATH_ERROR = Prefix('ERR')


def command(prefix: Prefix):
    """
    Вспомогательный декоратор для регистрации команд, принимаемых клиентом или сервером

    Добавляет функции атрибут с текстом команды
    """
    def register_command(f):
        f.prefix = prefix
        return f
    return register_command


class UDPClient:
    """
    Базовый класс для UDP-клиента, работающего в цикле

    UUID4 идентификатор клиента создаётся при создании экземпляра класса

    Формат входящего сообщения:
      ПРЕФИКС(аргумент 1, ... аргумент N)

    Формат исходящего сообщения
      UUID4 идентификатор|ПРЕФИКС(аргумент 1, ... аргумент N)

    """
    def __init__(self, ip_address: str, port: int):
        """
        Конструктор класса.
        Открывает сокет, выставляет необходимые параметры (в том числе идентификатор)
        """
        self.id_ = str(uuid4())
        self.buffer_size = BUFFER_SIZE
        self.server_address = ip_address, port
        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self._discover_processing_methods()
        self.finished = False
        self.socket.settimeout(CLIENT_TIMEOUT)

    def work(self):
        """
        Основной цикл работы клиента
        """
        while not self.finished:
            self.pre_loop()
            try:
                self._receive()
            except socket.timeout:
                self.on_timeout()

    def pre_loop(self):
        pass

    def on_timeout(self):
        """
        Действие, выполняемое по таймауту прослушивания порта
        """

    def _receive(self):
        """
        Метод получения входящего сообщения, парсинга этого сообщения и вызова соответствующего метода
        """
        data, address = self.socket.recvfrom(self.buffer_size)
        data_string = data.decode('utf-8')
        prefix = data_string.split('(')[0]
        args = ast.literal_eval(data_string[len(prefix):])
        method = self._methods[prefix]
        method(*args)

    def send(self, prefix: Prefix, *args):
        """
        Метод отправки сообщения
        """
        message = f'{self.id_}|{prefix}{args}'.encode('utf-8')
        self.socket.sendto(message, self.server_address)

    def _discover_processing_methods(self):
        """
        Поиск всех методов-обработчиков, с именем, начинающимся на "process_" и префиксом,
        определённым с помощью декоратора command.
        Вспомогательный метод, вызываемый из конструктора.
        """
        self._methods = {}
        for name in dir(self):
            if name.startswith('process_'):
                value = getattr(self, name)
                if prefix := getattr(value, 'prefix'):
                    self._methods[prefix] = value


class UDPServer:
    """
    Базовый класс для UDP-сервера, работающего в цикле


    Формат исходящего сообщения
      ПРЕФИКС(аргумент 1, ... аргумент N)

    Формат входящего сообщения:
      UUID4 идентификатор|ПРЕФИКС(аргумент 1, ... аргумент N)

    """
    def __init__(self, bind_address: str, bind_port: int):
        self.buffer_size = BUFFER_SIZE
        self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.socket.bind((bind_address, bind_port))
        self._discover_processing_methods()
        self.finished = False

    def _discover_processing_methods(self):
        """
        Поиск всех методов-обработчиков, с именем, начинающимся на "process_" и префиксом,
        определённым с помощью декоратора command.
        Вспомогательный метод, вызываемый из конструктора.
        """
        self._methods = {}
        for name in dir(self):
            if name.startswith('process_'):
                value = getattr(self, name)
                if prefix := getattr(value, 'prefix'):
                    self._methods[prefix] = value

    def work(self):
        """
        Основной цикл работы сервера
        """
        while not self.finished:
            self.pre_loop()
            self._receive_and_answer()

    def pre_loop(self):
        """
        Метод, выполняемый в начале каждой итерации основного цикла
        """

    def _receive_and_answer(self):
        """
        Метод получения входящего сообщения, парсинга этого сообщения и вызова соответствующего метода
        с последующей отправкой ответа клиенту, если ответ возвращён из метода
        """
        data, address = self.socket.recvfrom(self.buffer_size)
        data_string = data.decode('utf-8')
        client_id, data_raw = data_string.split('|')
        prefix = data_raw.split('(')[0]
        args = ast.literal_eval(data_raw[len(prefix):])
        method = self._methods[prefix]
        result = method(*args, client_id=client_id)
        if result is None:
            return
        if isinstance(result, str):
            prefix, args = result, ()
        else:
            prefix, *args = result
        self.send(address, prefix, *args)

    def send(self, address: Tuple[str, int], prefix: Prefix, *args):
        """
        Метод отправки сообщения
        """
        message = f'{prefix}{args}'.encode('utf-8')
        self.socket.sendto(message, address)
