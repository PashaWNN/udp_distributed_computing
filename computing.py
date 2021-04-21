import math
import time
from typing import Optional, Tuple

from integration import METHOD_NAMES
from utils import drange, validate_formula_code

WATCHDOG_TIMEOUT = 20


class Chunk:
    """
    Сущность, представляющая собой кусок (чанк) для дальнейшего вычисления
    Хранит в себе информацию о локальных пределах вычисления, ID клиента, занятого его вычислением,
    а также состояние в данный момент
    """
    def __init__(self, id_: int, lower_bound: float, higher_bound: float):
        """
        Конструктор чанка

        :param id_: ID чанка, по сути являющийся его порядковым номером
        :param lower_bound: нижняя граница интегрирования внутри чанка
        :param higher_bound: верхняя граница интегрирования внутри чанка
        """
        self.id = id_
        self._allocated = False
        self._finished = False
        self._bounds = lower_bound, higher_bound
        self._last_message_time = time.time()

    def allocate(self):
        """
        Обозначить блок как занятый клиентом и обновить время последнего обращения к нему
        """
        self.reset_watchdog()
        self._allocated = True

    def deallocate(self, finished=True):
        """
        Пометить блок как свободный

        :param finished: нужно ли помечать блок ещё и как завершённый
        """
        self._allocated = False
        self._finished = finished

    def deallocate_if_not_alive(self):
        """
        Пометить как свободный для вычисления, если в течение долгого времени не было
        обращений
        """
        if not self._allocated:
            return
        if self._last_message_time + WATCHDOG_TIMEOUT < time.time():
            print(f'Назначение чанка #{self.id} снято по таймауту')
            self.deallocate(finished=False)

    def reset_watchdog(self) -> None:
        """
        :returns: время последнего обращения
        """
        self._last_message_time = time.time()

    @property
    def is_finished(self) -> bool:
        """
        :returns: состояние завершённости
        """
        return self._finished

    @property
    def is_free(self) -> bool:
        """
        :returns: состояние занятости клиентом
        """
        return not self._allocated

    def get_bounds(self) -> Tuple[float, float]:
        """
        :returns: локальные пределы вычисления
        """
        return self._bounds

    def __repr__(self):
        return f'Чанк ({self._bounds})'


class DistributedComputingController:
    """
    Контроллер распределённых вычислений. Определяет основную логику вычислений
    """
    def __init__(
            self,
            lower_bound: float,
            higher_bound: float,
            formula: str,
            computation_method: str,
            cluster_count: int = 5
    ):
        self.cluster_ids_count = 0
        self._clients_to_chunks_map = {}
        self._chunks = []
        self.finished = False
        self.cluster_count = cluster_count
        self.computation_method = computation_method
        self._result = 0.0

        # Определить шаг и разбить предел на локальные пределы и создать Чанки для них
        step = (higher_bound - lower_bound) / cluster_count
        for i, lbound in enumerate(drange(lower_bound, higher_bound, step)):
            self._chunks.append(
                Chunk(i, lbound, lbound + step)
            )

        self.formula = formula
        # Проверить формулу
        valid = validate_formula_code(formula)
        if not valid:
            raise ValueError('Получена некорректная формула')

    @property
    def cluster_args(self):
        """
        :returns: список общих аргументов, которые необходимы кластеру для начала работы
            А именно, необходимо сообщить клиенту формулу и метод вычислений (кроме локальных
            пределов вычисления, но они берутся из конкретного чанка, назначенного клиенту)
        """
        return f"{self.formula}", f"{self.computation_method}"

    def allocate_chunk(self, client_id: str) -> Optional[Chunk]:
        """
        Получить свободный чанк и пометить его как занятый

        :returns: чанк, если есть подходящий
        """
        chunk = self._get_free_chunk()
        if not chunk:
            return
        self._clients_to_chunks_map[client_id] = chunk.id
        chunk.allocate()
        return chunk

    def _get_free_chunk(self) -> Optional[Chunk]:
        """
        Получить первый попавшийся свободный и незавершённый чанк

        :returns: чанк, если есть подходящий
        """
        for chunk in self._chunks:
            if chunk.is_free and not chunk.is_finished:
                return chunk

    def add_result_part(self, client_id: str, result_part: float) -> Optional[float]:
        """
        Добавить часть результата к общему результату вычислений,
        пометить соответствующий чанк как законченный, а также
        завершить работу и вернуть результат, если не осталось незаконченных чанков

        :param client_id: ID клиента
        :param result_part: частичный результат, полученный клиентом
        :returns: результат вычислений, если вычисления завершены, иначе ничего
        """
        chunk_id = self._clients_to_chunks_map.get(client_id)
        if chunk_id is None:
            return
        self._result += result_part
        self._chunks[chunk_id].deallocate()
        if all((chunk.is_finished for chunk in self._chunks)):
            self.finished = True
            return self._result

    def deallocate_abandoned_chunks(self):
        """
        Пройтись по чанкам и пометить как свободные те, которые долго не получали обращений

        :returns: ничего
        """
        for chunk in self._chunks:
            chunk.deallocate_if_not_alive()

    def reset_watchdog(self, client_id):
        """
        Обратиться к чанку, назначенному соответствующему клиенту, чтобы сбросить таймер

        :param client_id: ID клиента
        :returns: ничего
        """
        chunk_id = self._clients_to_chunks_map.get(client_id)
        if chunk_id is not None:
            self._chunks[chunk_id].reset_watchdog()


class DistributionComputingClient:
    """
    Контроллер клиента вычислений
    """
    def __init__(self):
        self.formula = None
        self.method = None
        self.lower_bound = None
        self.higher_bound = None

    def set_task(self, method_name: str, formula: str, lower_bound: float, higher_bound: float):
        """
        Установить задание для вычисления

        :param method_name: метод вычисления, SIM или TRA
        :param formula: формула для вычисления
        :param lower_bound: нижний предел
        :param higher_bound: верхний предел
        """
        formula = formula.replace('^', '**')
        valid = validate_formula_code(formula)
        if not valid:
            raise ValueError('Получена некорректная формула')
        self.formula = eval(f'lambda x: {formula}', {'sqrt': math.sqrt})
        self.method = METHOD_NAMES[method_name]
        self.lower_bound = lower_bound
        self.higher_bound = higher_bound

    def compute(self):
        """
        Вычислить согласно установленному заданию
        :return: частичный результат вычисления
        """
        # Подождём пару секунд. Это нужно для наглядности работы всей системы
        # Иначе вычисляется слишком быстро и кластеры не успевают равномерно разобрать себе задания
        time.sleep(2)
        return self.method(self.formula, self.lower_bound, self.higher_bound)

