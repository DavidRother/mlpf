import random
import pandas as pd
from typing import List, Tuple, Dict, Any


def random_order(data_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]]) -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    return random.sample(data_frames, len(data_frames))


def sort(data_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]], order_keys: List[str]) \
        -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
    return sorted(data_frames, key=lambda x: [x[1][order_key] for order_key in order_keys])
