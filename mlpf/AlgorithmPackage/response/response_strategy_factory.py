from . import abstract_response_strategy
from abc import ABCMeta
from typing import List, Dict


__dispatcher: Dict[str, ABCMeta] = {}


def create_response_strategy(method_name: str) -> abstract_response_strategy.AbstractResponseStrategy:
    return __dispatcher[method_name]()


def get_selection() -> List[str]:
    return list(__dispatcher.keys())


def register(method_name: str, response_strategy_class: ABCMeta):
    if method_name in __dispatcher:
        raise ValueError('Response strategy with this name already exists.')
    __dispatcher[method_name] = response_strategy_class

