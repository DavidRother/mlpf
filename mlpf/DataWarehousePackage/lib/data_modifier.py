from .data_table import AbstractDataTable
from ..lib import data_aggregation
from ...AlgorithmPackage.preprocessing.abstract_preprocessor import AbstractPreprocessor
from typing import List, Tuple, Any, Union
import pandas as pd
from copy import deepcopy


def apply_preprocessing(data_tables: List[AbstractDataTable], preprocessor: AbstractPreprocessor, source_names: List[str],
                        mark_new: Tuple[str, Any], mark_old: Union[Tuple[str, Any], None],
                        batch_mode: bool) -> List[AbstractDataTable]:
    if batch_mode:
        return _batch_preprocessing(data_tables, preprocessor, source_names, mark_new, mark_old)
    else:
        preprocessed_tables = []
        for data_table in data_tables:
            new_table = _iterative_preprocessing(data_table, preprocessor, source_names, mark_new, mark_old)
            preprocessed_tables.append(new_table)
        return preprocessed_tables


def _batch_preprocessing(data_tables: List[AbstractDataTable], preprocessor: AbstractPreprocessor,
                         source_names: List[str],
                         mark_new: Tuple[str, Any], mark_old: Union[Tuple[str, Any], None]) -> List[AbstractDataTable]:
    preprocessed_tables = []
    new_frames = preprocessor.preprocess_batch_ordered([data_aggregation.select_columns(data_table, source_names)
                                                        for data_table in data_tables])
    for frame, old_table in zip(new_frames, data_tables):
        if mark_old:
            old_table.add_meta_data_key(mark_old[0], mark_old[1])
        new_table = _inherit_table(old_table, mark_new, frame)
        preprocessed_tables.append(new_table)
    return preprocessed_tables


def _iterative_preprocessing(data_table: AbstractDataTable, preprocessor: AbstractPreprocessor, source_names: List[str],
                             mark_new: Tuple[str, Any], mark_old: Union[Tuple[str, Any], None]) -> AbstractDataTable:
    preprocessing_frame = data_aggregation.select_columns(data_table, source_names)
    new_frame = preprocessor.preprocess(preprocessing_frame)
    if mark_old:
        data_table.add_meta_data_key(mark_old[0], mark_old[1])
    return _inherit_table(data_table, mark_new, new_frame)


def _inherit_table(old_table: AbstractDataTable, meta_extension: Tuple[str, Any], data_frame: pd.DataFrame):
    meta_copy = deepcopy(old_table.meta_data_keys)
    meta_dict_copy = deepcopy(old_table.meta_data)
    new_table = AbstractDataTable.from_frame(data_frame, meta_dict_copy, meta_copy)
    new_table.add_meta_data_key(*meta_extension)
    return new_table
