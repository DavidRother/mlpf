from .data_table import DataTable
from typing import List, Callable, Tuple
import pandas as pd
import functools


def select_columns(data_table: DataTable, columns: List[str]) -> pd.DataFrame:
    df = data_table.frame
    return df.loc[:, columns]


def select_columns_rows(data_table: DataTable, columns: List[str], row_filter: pd.Series) -> pd.DataFrame:
    df = data_table.frame
    return df.loc[row_filter, columns]


def filter_rows(data_table: DataTable, row_filter: pd.Series) -> pd.DataFrame:
    df = data_table.frame
    return df.loc[row_filter, :]


def create_mask(data_frame: pd.DataFrame, selectors: List[Tuple[str, Callable]]) -> pd.Series:
    return functools.reduce(lambda x, y: x & y, (fun(data_frame[column]) for column, fun in selectors))

