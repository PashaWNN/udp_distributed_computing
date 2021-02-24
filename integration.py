from typing import Callable


def _rectangle_rule(func: Callable[[float], float], a: float, b: float, segment_count: int, frac: float) -> float:
    """
    Обобщённое правило прямоугольников.

    """
    dx = 1.0 * (b - a) / segment_count
    sum_ = 0.0
    x_start = a + frac * dx
    # 0 <= frac <= 1 задаёт долю смещения точки,
    # в которой вычисляется функция,
    # от левого края отрезка dx
    for i in range(segment_count):
        sum_ += func(x_start + i * dx)

    return sum_ * dx


def left_rectangle_rule(func: Callable[[float], float], a: float, b: float, segment_count: int):
    """Правило левых прямоугольников"""
    return _rectangle_rule(func, a, b, segment_count, 0.0)


def right_rectangle_rule(func: Callable[[float], float], a: float, b: float, segment_count: int):
    """Правило правых прямоугольников"""
    return _rectangle_rule(func, a, b, segment_count, 1.0)


def midpoint_rectangle_rule(func: Callable[[float], float], a: float, b: float, segment_count: int):
    """Правило прямоугольников со средней точкой"""
    return _rectangle_rule(func, a, b, segment_count, 0.5)


def trapezoid_rule(func: Callable[[float], float], a: float, b: float, precision=1e-12, initial_segment_count=1):
    """
    Правило трапеций

    :param func: интегрируемая функция
    :param a: нижний предел
    :param b: верхний предел
    :param precision: - желаемая относительная точность вычислений
    :param initial_segment_count: - начальное число отрезков разбиения
    """
    n_seg = initial_segment_count
    dx = 1.0 * (b - a) / n_seg
    ans = 0.5 * (func(a) + func(b))
    for i in range(1, n_seg):
        ans += func(a + i * dx)

    ans *= dx
    err_est = max(1.0, abs(ans))

    while err_est > abs(precision * ans):
        old_ans = ans
        ans = 0.5 * (ans + midpoint_rectangle_rule(func, a, b, n_seg))
        # новые точки для уточнения интеграла
        # добавляются ровно в середины предыдущих отрезков
        n_seg *= 2
        err_est = abs(ans - old_ans)

    return ans


def simpson_rule(func: Callable[[float], float], a: float, b: float, segment_count: int = 500) -> float:
    """
    Правило Симпсона
    :param func: интегрируемая функция
    :param a: нижний предел
    :param b: верхний предел
    :param segment_count: число отрезков разбиения
    """
    if segment_count % 2 == 1:
        segment_count += 1
    dx = 1.0 * (b - a) / segment_count
    sum_ = (func(a) + 4 * func(a + dx) + func(b))
    for i in range(1, segment_count // 2):
        sum_ += 2 * func(a + (2 * i) * dx) + 4 * func(a + (2 * i + 1) * dx)

    return sum_ * dx / 3


METHOD_NAMES = {
    'TRA': trapezoid_rule,
    'SIM': simpson_rule,
}
