import pandas as pd
import logging

from typing import List, Dict, Any
from abc import abstractstaticmethod, abstractclassmethod, ABC


class AbstractDataTable(ABC):

    def __init__(self, data_frame: pd.DataFrame, meta_dict: Dict[str, Any], meta_data_keys: List[str] = None):
        self.meta_data_keys: List[str] = meta_data_keys or []
        self.meta_data: Dict[str, Any] = meta_dict or {}
        self.frame: pd.DataFrame = data_frame

    def add_meta_data_key(self, new_key: str, new_value: Any):
        if new_key in self.meta_data_keys:
            logging.warning('Did not add meta data key {} as it already exists.'.format(new_key))
            return
        self.meta_data_keys.append(new_key)
        self.meta_data[new_key] = new_value

    def compare_meta_data(self, inclusive: Dict[str, Any], exclusive: Dict[str, Any]) -> bool:
        for k, v in inclusive.items():
            try:
                if self.meta_data[k] != v:
                    return False
            except KeyError:
                return False
        for exclusive_key, excluded_value in exclusive.items():
            if exclusive_key in self.meta_data:
                if self.meta_data[exclusive_key] == excluded_value:
                    return False
                if excluded_value is None:
                    return False
        return True

    @classmethod
    def from_source(cls, data_source: str, meta_data_keys: List[str] = None):
        meta_data: Dict[str, Any] = cls._load_meta_data(data_source, meta_data_keys or [])
        frame: pd.DataFrame = cls._load_frame(data_source)
        return cls(frame, meta_data, meta_data_keys)

    @classmethod
    def from_frame(cls, data_frame: pd.DataFrame, meta_dict: Dict[str, Any], meta_keys: List[str]):
        return cls(data_frame, meta_dict, meta_keys)

    @abstractclassmethod
    def _load_meta_data(cls, data_source: str, meta_data_keys: List[str]) -> Dict[str, Any]:
        raise NotImplementedError()

    @abstractstaticmethod
    def _load_frame(data_source: str) -> pd.DataFrame:
        raise NotImplementedError()
