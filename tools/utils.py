# -*- coding: utf-8 -*-

from datetime import date
import logging
import os
import inspect
import json
import traceback
from types import ModuleType
from importlib import import_module
from pkgutil import iter_modules


def initlog(tag: str, receivers = ['/dev/log'], kafka: dict = None, level=logging.INFO, console=False):
    """
    初始化日志
    :param tag
    :parma receivers
    :return
    """
    handlers = []
    import socket
    formatter = logging.Formatter(f'1 - {socket.gethostname()} {tag}_%(name)s - - : %(filename)s[%(lineno)d] %(message)s')
    from .handlers import RsyslogHandler
    for receiver in receivers:
        handler = RsyslogHandler(address=receiver)
        handler.formatter = formatter
        handlers.append(handler)

    if console:
        stream = logging.StreamHandler()
        handlers.append(stream)

    if kafka:
        from .handlers import KafkaHandler
        handlers.append(KafkaHandler(tag, config=kafka))

    logging.basicConfig(
        level=level,
        format=r'%(asctime)s %(levelname)s %(name)s: %(filename)s[%(lineno)d] %(message)s',
        datefmt=r'%Y-%m-%d %H:%M:%S',
        handlers=handlers)


def walk_modules(path):
    mods = []
    mod = import_module(path)
    # mods.append(mod)
    if hasattr(mod, '__path__'):
        for _, subpath, ispkg in iter_modules(mod.__path__):
            fullpath = path + '.' + subpath
            if ispkg:
                mods += walk_modules(fullpath)
            else:
                submod = import_module(fullpath)
                mods.append(submod)
    return mods


def load_modules(module) -> dict:
    mods = {}
    dirname = os.path.dirname(inspect.getfile(module))
    basename = module.__module__
    for _, subpath, ispkg in iter_modules([dirname]):
        if not ispkg:
            mod = import_module(basename + '.' + subpath)
            for obj in vars(mod).values():
                if inspect.isclass(obj) and issubclass(obj, module) and not obj == module and hasattr(obj, '__type__'):
                    mods[obj.__type__] = obj
    return mods


def load_modules_from_path(package: ModuleType, base) -> list:
    mods = []
    basename = package.__package__
    dirname = f"{os.getcwd()}/{basename}"
    for _, subpath, ispkg in iter_modules([dirname]):
        mod = import_module(basename + '.' + subpath)
        if not ispkg:
            mod = import_module(basename + '.' + subpath)
            for obj in vars(mod).values():
                if inspect.isclass(obj) and issubclass(obj, base) and not obj == base:
                    mods.append(obj)
    return mods

