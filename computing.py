import ast
import math
import time
from typing import Optional, Tuple

from integration import METHOD_NAMES

WATCHDOG_TIMEOUT = 20


def validate_formula_code(formula):
    whitelist = (
        ast.Expression, ast.Call, ast.Name, ast.Load, ast.BinOp,
        ast.UnaryOp, ast.operator, ast.unaryop, ast.cmpop, ast.Num,
    )

    tree = ast.parse(formula, mode='eval')
    return all(isinstance(node, whitelist) for node in ast.walk(tree))


class Chunk:
    def __init__(self, id_: int, lower_bound: float, higher_bound: float):
        self.id = id_
        self._allocated = False
        self._finished = False
        self._bounds = lower_bound, higher_bound
        self._last_message_time = time.time()

    def allocate(self):
        self._last_message_time = time.time()
        self._allocated = True

    def deallocate(self, finished=True):
        self._allocated = False
        self._finished = finished

    def deallocate_if_not_alive(self):
        if not self._allocated:
            return
        if self._last_message_time + WATCHDOG_TIMEOUT < time.time():
            print(f'Назначение чанка #{self.id} снято по таймауту')
            self.deallocate(finished=False)

    def reset_watchdog(self):
        self._last_message_time = time.time()

    @property
    def is_finished(self):
        return self._finished

    @property
    def is_free(self):
        return not self._allocated

    def get_bounds(self) -> Tuple[float, float]:
        return self._bounds

    def __repr__(self):
        return f'Чанк ({self._bounds})'


def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step


class DistributedComputingController:
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
        self.finished = False
        self.cluster_count = cluster_count
        self._chunks = []

        self._result = float(0)
        step = (higher_bound - lower_bound) / cluster_count
        for i, lbound in enumerate(drange(lower_bound, higher_bound, step)):
            self._chunks.append(Chunk(i, lbound, lbound + step))
        self.formula = formula
        valid = validate_formula_code(formula)
        if not valid:
            raise ValueError('Получена некорректная формула')
        formula_compiled = eval(f'lambda x: {formula}', {'sqrt': math.sqrt})
        try:
            formula_compiled(10.0)
        except Exception as e:
            raise ValueError(f'Ошибка вычислений в формуле: {str(e)}')
        self.computation_method = computation_method

    @property
    def cluster_args(self):
        return f"{self.formula}", f"{self.computation_method}"

    def _get_free_chunk(self) -> Optional[Chunk]:
        for chunk in self._chunks:
            if chunk.is_free and not chunk.is_finished:
                return chunk

    def allocate_chunk(self, client_id: str) -> Optional[Chunk]:
        chunk = self._get_free_chunk()
        if not chunk:
            return
        self._clients_to_chunks_map[client_id] = chunk.id
        chunk.allocate()
        return chunk

    def add_result_part(self, client_id: str, result_part: float) -> Optional[float]:
        chunk_id = self._clients_to_chunks_map.get(client_id)
        if chunk_id is None:
            return
        self._result += result_part
        self._chunks[chunk_id].deallocate()
        if all((chunk.is_finished for chunk in self._chunks)):
            self.finished = True
            return self._result

    def deallocate_abandoned_chunks(self):
        for chunk in self._chunks:
            chunk.deallocate_if_not_alive()

    def reset_watchdog(self, client_id):
        chunk_id = self._clients_to_chunks_map.get(client_id)
        if chunk_id is not None:
            self._chunks[chunk_id].deallocate_if_not_alive()


class DistributionComputingClient:
    def __init__(self):
        self.formula = None
        self.method = None
        self.lower_bound = None
        self.higher_bound = None

    def set_task(self, method_name: str, formula: str, lower_bound: float, higher_bound: float):
        formula = formula.replace('^', '**')
        valid = validate_formula_code(formula)
        if not valid:
            raise ValueError('Получена некорректная формула')
        self.formula = eval(f'lambda x: {formula}', {'sqrt': math.sqrt})
        self.method = METHOD_NAMES[method_name]
        self.lower_bound = lower_bound
        self.higher_bound = higher_bound

    def compute(self):
        time.sleep(2)
        return self.method(self.formula, self.lower_bound, self.higher_bound)

