from . import data_sort
import pandas as pd
import random
from typing import List, Tuple, Dict, Any, Callable
from collections import defaultdict


def plan_random(data_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]], seed=1) \
        -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    if seed:
        random.seed(seed)
    tmp = data_sort.random_order(data_frames)
    return tmp


dispatcher = defaultdict(lambda: plan_random, {'random': plan_random})


def get_data_plan(learning_plan: str) -> Callable:
    return dispatcher[learning_plan]
