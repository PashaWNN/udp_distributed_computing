import ast
import re


def tk_validator(root, func):
    def validate(action, index, value_if_allowed, prior_value, text, validation_type, trigger_type, widget_name):
        if value_if_allowed:
            return func(value_if_allowed)
        return True
    return root.register(validate), '%d', '%i', '%P', '%s', '%S', '%v', '%V', '%W'


def validate_float(value):
    try:
        if value == '-': return True
        float(value)
        return True
    except ValueError:
        return False


def validate_int(value):
    try:
        val = int(value)
        return True
    except ValueError:
        pass
    return False


_ip_regex = re.compile(r'^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\.(?!$)|$)){4}$')


def validate_ip(value):
    return bool(_ip_regex.match(value))


def validate_port(value):
    try:
        val = int(value)
        return 65536 > val > 0
    except ValueError:
        pass
    return False


def drange(start, stop, step):
    """
    Вспомогательная функция-генератор, возвращающая значения от start до stop с шагом step
    """
    r = start
    while r < stop:
        yield r
        r += step


def validate_formula_code(formula):
    """
    Функция для проверки корректности формулы

    :returns: True если формула корректна, иначе False
    """
    whitelist = (  # определяем список разрешённых операторов
        ast.Expression, ast.Call, ast.Name, ast.Load, ast.BinOp,
        ast.UnaryOp, ast.operator, ast.unaryop, ast.cmpop, ast.Num,
    )

    tree = ast.parse(formula, mode='eval')  # разберём код формулы на части
    # вернём True, если все части входят в белый список
    return all(isinstance(node, whitelist) for node in ast.walk(tree))
