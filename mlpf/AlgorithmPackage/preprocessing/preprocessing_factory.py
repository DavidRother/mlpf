from .abstract_preprocessor import AbstractPreprocessor
from abc import ABCMeta
from typing import List, Dict


__dispatcher: Dict[str, ABCMeta] = {}


def create_preprocessor(method_name: str, settings: Dict) -> AbstractPreprocessor:
    return __dispatcher[method_name](**settings)


def get_selection() -> List[str]:
    return list(__dispatcher.keys())


def register(method_name: str, response_strategy_class: ABCMeta):
    if method_name in __dispatcher:
        raise ValueError('Response strategy with name {} already exists.'.format(method_name))
    __dispatcher[method_name] = response_strategy_class
