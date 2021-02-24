import tkinter as tk
from tkinter import ttk
from typing import Callable, Type, Dict, Any

from utils import tk_validator

DEFAULT_WIDTH = 20


class Component:
    widget_type: Type[tk.Widget]

    def __init__(self, title: str = '', **kwargs):
        self.title = title
        self.additional_kwargs = kwargs

    def get_kwargs(self, root) -> Dict[str, Any]:
        return {'width': DEFAULT_WIDTH}

    def produce_widget(self, window: 'Window', root: tk.Tk) -> tk.Widget:
        return self.widget_type(root, **self.additional_kwargs)


class Button(Component):
    widget_type = tk.Button

    def __init__(self, text: str, command: str, title: str = '', width: int = DEFAULT_WIDTH):
        super().__init__(title, text=text)
        self.command = command

    def produce_widget(self, window: 'Window', root: tk.Tk) -> tk.Widget:
        command_fn = getattr(window, self.command)
        return self.widget_type(command=command_fn, **self.additional_kwargs)


class Entry(Component):
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
    widget_type = ttk.Combobox


class TextArea(Component):
    widget_type = tk.Text


class Window:

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

    def __init__(self, title: str, window: Type[Window]):
        self.root = tk.Tk()
        self.root.title(title)
        self.thread = None
        self.current_window = None
        self.context = {}
        self.set_window_contents(window)
        self.root.protocol("WM_DELETE_WINDOW", self.handle_exit)
        self.events = {}

    def mainloop(self):
        self.root.mainloop()

    def emit_event(self, event_name, *args, **kwargs):
        for subscriber in self.events[event_name]:
            subscriber.receive_event(event_name, *args, **kwargs)

    def subscribe(self, event_name, window: Window):
        self.events.setdefault(event_name, []).append(window)

    def set_window_contents(self, window: Type[Window]):
        self.clear_window()
        self.current_window = window(self)

    def clear_window(self):
        for item in self.root.winfo_children():
            item.destroy()

    def handle_exit(self):
        if self.thread is not None:
            self.thread.do_work = False
        exit()


class LoggerWindow(Window):
    log = TextArea(width=300)

    def __init__(self, app):
        super().__init__(app)
        self.app.context['logger'] = self.add_entry
        self.app.subscribe('log', self)

    def receive_event(self, event_name, *args, **kwargs):
        self.add_entry(*args)

    def add_entry(self, text: str):
        self['log'].insert(tk.END, text + '\n')
