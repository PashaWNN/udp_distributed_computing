import sys
import tkinter as tk
from tkinter import ttk
from typing import Callable, Type, Dict, Any

from utils import tk_validator

DEFAULT_WIDTH = 20


class Component:
    """
    Базовый класс, определяющий логику создания компонента
    """
    widget_type: Type[tk.Widget]

    def __init__(self, title: str = '', **kwargs):
        self.title = title
        self.additional_kwargs = kwargs

    def get_kwargs(self, root) -> Dict[str, Any]:
        return {'width': DEFAULT_WIDTH}

    def produce_widget(self, window: 'Window', root: tk.Tk) -> tk.Widget:
        return self.widget_type(root, **self.additional_kwargs)


class Button(Component):
    """
    Класс, определяющий логику создания кнопки
    """
    widget_type = tk.Button

    def __init__(self, text: str, command: str, title: str = '', width: int = DEFAULT_WIDTH):
        super().__init__(title, text=text)
        self.command = command

    def produce_widget(self, window: 'Window', root: tk.Tk) -> tk.Widget:
        command_fn = getattr(window, self.command)
        return self.widget_type(command=command_fn, **self.additional_kwargs)


class Entry(Component):
    """
    Класс, определяющий логику создания текстового поля
    """
    widget_type = tk.Entry

    def __init__(self, title: str = '', default: str = None, validator: Callable = None, **kwargs):
        super().__init__(title, **kwargs)
        self.default = default
        self.validator = validator

    def produce_widget(self, window: 'Window', root: tk.Tk) -> tk.Entry:
        kwargs = self.get_kwargs(root)

        widget = self.widget_type(root, **kwargs)
        if self.default:
            widget.insert(0, self.default)
        return widget

    def get_kwargs(self, root):
        kwargs = super().get_kwargs(root)
        kwargs.update(self.additional_kwargs)
        if self.validator is not None:
            kwargs['validatecommand'] = tk_validator(root, self.validator)
            kwargs['validate'] = 'key'
        return kwargs


class Combobox(Entry):
    """
    Класс, определяющий логику создания выпадающего списка
    (копирует логику текстового поля, меняя только тип компонента)
    """
    widget_type = ttk.Combobox


class TextArea(Component):
    """
    Класс, определяющий логику многострочного текстового поля
    (не требует дополнительной логики)
    """
    widget_type = tk.Text


class Window:
    """
    Базовый класс окна, позволяющий упростить создание окон с компонентами

    В конструкторе определна логика получения списка компонентов из тела класса
    с последующим расположением этих компонентов на форме в виде сетки с подписями слева

    Также имеет метод receive_event, позволяющий обрабатывать события в приложении
    """
    def __init__(self, app: 'Application'):
        self.app = app
        self.widgets = {}
        grid = []
        for name, component in self.__class__.__dict__.items():
            if not isinstance(component, Component):
                continue
            widget = component.produce_widget(self, self.app.root)
            grid.append(
                [tk.Label(text=component.title), widget]
            )
            self.widgets[name] = widget
        for i, row in enumerate(grid):
            for j, col in enumerate(row):
                col.grid(row=i, column=j)

    def __getitem__(self, item):
        return self.widgets[item]

    def receive_event(self, event_name, *args, **kwargs):
        pass


class Application:
    """
    Базовый класс графического приложения, реализующий логику работы с окнами и компонентами
    """
    def __init__(self, title: str, window: Type[Window]):
        """
        Конструктор

        :param title: заголовок окна
        :param window: начальное окно
        """
        self.root = tk.Tk()
        self.root.title(title)
        self.thread = None
        self.current_window = None
        self.set_window_contents(window)
        self.root.protocol("WM_DELETE_WINDOW", self.handle_exit)
        self.events = {}

    def mainloop(self):
        self.root.mainloop()

    def emit_event(self, event_name, *args, **kwargs):
        """
        Рассылка события всем подписавшимся на данный тип события

        :param event_name: имя (тип) события
        :param args: порядковые аргументы события
        :param kwargs: словарные аргументы события
        """
        for subscriber in self.events.get(event_name, []):
            subscriber.receive_event(event_name, *args, **kwargs)

    def subscribe(self, event_name, window: Window):
        """
        Метод "подписаться на событие". Вызывается из класса окна

        :param event_name: имя событие, на которое окно хочет подписаться
        :param window: окно, которое хочет получать события с указанным именем
        """
        self.events.setdefault(event_name, []).append(window)

    def set_window_contents(self, window: Type[Window]):
        """
        Очистить холст и заполнить его компонентами нового окна window

        :param window: класс нового окна
        """
        self.clear_window()
        self.current_window = window(self)

    def clear_window(self):
        """
        низкоуровневый метод очистки холста от старых компонентов
        """
        for item in self.root.winfo_children():
            item.destroy()

    def handle_exit(self):
        """
        обработчик нажатия на "крестик"
        Устанавливает флаг прекращения работы потоку, если поток запущен и выходит
        """
        self.emit_event('log', 'Завершение работы...')
        if self.thread is not None:
            self.thread.do_work = False
        sys.exit()


class LoggerWindow(Window):
    """
    Общий класс окна с большим текстовым полем, используемый и в клиенте, и в сервере
    """
    log = TextArea(width=300)

    def __init__(self, app):
        super().__init__(app)
        self.app.subscribe('log', self)  # подписаться на событие `log`

    def receive_event(self, event_name, *args, **kwargs):
        """
        Обработчик событий
        :param event_name: имя произошедешго события
        :param args: порядковые аргументы события
        :param kwargs: словарные аргументы события
        """
        if event_name == 'log':
            self.add_entry(*args)

    def add_entry(self, text: str):
        """
        Метод вставки новой строки в текстовое поле

        :param text: текст для вставки
        """
        self['log'].insert(tk.END, text + '\n')
