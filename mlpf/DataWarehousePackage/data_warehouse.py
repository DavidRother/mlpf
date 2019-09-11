from .lib.data_table import AbstractDataTable
from .data_store import DataStore
from .lib import data_aggregation, data_modifier
from ..AlgorithmPackage.preprocessing import preprocessing_factory
from ..AlgorithmPackage.preprocessing.abstract_preprocessor import AbstractPreprocessor
from typing import List, Tuple, Callable, Union, Dict, Any
import pandas as pd
import os
import uuid


class DataWarehouse:

    def __init__(self, data_root: str):
        self._data_store: DataStore = DataStore()
        self._data_root: str = data_root
        self._preprocesser_store: Dict[str, AbstractPreprocessor] = {}

    def load_data_folders(self, folders=List[str], meta_data_keys: Union[List[str], None] = None) -> List[str]:
        """
        Load all csv files in a folder

        :param folders:
        :param meta_data_keys:
        :return:
        """
        table_ids = []
        for folder in folders:
            for file in os.listdir(os.path.join(self._data_root, folder)):
                if not file.endswith('.csv'):
                    continue
                table_id = self.load_data_file(os.path.join(self._data_root, folder, file), meta_data_keys)
                table_ids.append(table_id)
        return table_ids

    def load_data_file(self, file: str, meta_data_keys: Union[List[str], None]) -> str:
        """
        Load a csv file into a data table structure

        :param file:
        :param meta_data_keys:
        :return:
        """
        if not file.endswith('.csv'):
            raise ValueError('specified file has to be a .csv file')
        table_id = str(uuid.uuid4())
        self._data_store.add_source(table_id, file, meta_data_keys)
        return table_id

    def preprocessing_by_args(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]], method_name: str,
                              source_names: List[str], settings: Dict, mark_new: Tuple[str, Any],
                              mark_old: Union[Tuple[str, Any], None], batch_mode: bool) -> List[str]:
        """
        Preprocess all data tables that fit the filters.

        :param meta_filter:
        :param method_name:
        :param source_names:
        :param settings:
        :param mark_new:
        :param mark_old:
        :param batch_mode:
        :return:
        """
        data_tables = self._retrieve_data_by_args(meta_filter)
        preprocessor = self._get_preprocessor(method_name, settings)
        new_tables = data_modifier.apply_preprocessing(data_tables, preprocessor, source_names,
                                                       mark_new, mark_old, batch_mode)
        return [self._add_table(table) for table in new_tables]

    def preprocessing_by_id(self, table_ids: List[str], method_name: str, source_names: List[str],
                            settings: Dict, mark_new: Tuple[str, Any],
                            mark_old: Union[Tuple[str, Any], None], batch_mode: bool) -> List[str]:
        """
        Preprocess all data tables specified in the id list.

        :param table_ids:
        :param method_name:
        :param source_names:
        :param settings:
        :param mark_new:
        :param mark_old:
        :param batch_mode:
        :return:
        """
        data_tables = [self._retrieve_data_by_id(table_id) for table_id in table_ids]
        preprocessor = self._get_preprocessor(method_name, settings)
        new_tables = data_modifier.apply_preprocessing(data_tables, preprocessor, source_names,
                                                       mark_new, mark_old, batch_mode)
        return [self._add_table(table) for table in new_tables]

    def get_data_by_id(self, table_id: str, columns: Union[List[str], None] = None,
                       row_filter: Union[List[Tuple[str, Callable]], None] = None) -> pd.DataFrame:
        """
        Retrieve data of the data table id.

        :param table_id:
        :param columns:
        :param row_filter:
        :return:
        """
        dt = self._retrieve_data_by_id(table_id)
        return self._apply_filter(dt, columns, row_filter)

    def get_data_by_meta_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]],
                              columns: Union[List[str], None] = None,
                              row_filter: Union[List[Tuple[str, Callable]], None] = None) -> List[pd.DataFrame]:
        """
        Retrieve the data that matches the filter.

        :param meta_filter:
        :param columns:
        :param row_filter:
        :return:
        """
        dt_list = self._retrieve_data_by_args(meta_filter)
        return [self._apply_filter(dt, columns, row_filter) for dt in dt_list]

    def get_complete_data_by_meta_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]],
                                       columns: Union[List[str], None] = None,
                                       row_filter: Union[List[Tuple[str, Callable]], None] = None) \
            -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
        """
        Retrieve the complete data tables by matching filters.

        :param meta_filter:
        :param columns:
        :param row_filter:
        :return:
        """
        dt_list = self._retrieve_data_by_args(meta_filter)
        return [self._apply_filter_full_return(dt, columns, row_filter) for dt in dt_list]

    def add_meta_data(self, table_id: str, meta_data_update: Dict[str, Any]):
        """
        Enrich a data table with new meta data

        :param table_id:
        :param meta_data_update:
        :return:
        """
        for k, v in meta_data_update.items():
            self._data_store.add_meta_data(table_id, (k, v))

    def reset_preprocessor(self, method_name: str):
        """
        Reset the state of the Preprocessor

        :param method_name:
        :return:
        """
        # TODO: Change the behaviour to only resetting and not removing the preprocessing method
        self._preprocesser_store.pop(method_name, None)

    def update_preprocessor_settings(self, method_name: str, new_settings: Dict):
        """
        Update the settings of the specified preprocessor

        :param method_name:
        :param new_settings:
        :return:
        """
        if method_name not in self._preprocesser_store:
            return
        self._preprocesser_store[method_name].update_settings(**new_settings)

    def _get_preprocessor(self, method_name: str, settings: Dict):
        if method_name not in self._preprocesser_store:
            self._preprocesser_store[method_name] = preprocessing_factory.create_preprocessor(method_name, settings)
        return self._preprocesser_store[method_name]

    def _add_table(self, data_table: AbstractDataTable) -> str:
        table_id = str(uuid.uuid4())
        self._data_store.add_table(table_id, '', data_table)
        return table_id

    def _retrieve_data_by_id(self, table_id: str) -> AbstractDataTable:
        return self._data_store.load_by_id(table_id)

    def _retrieve_data_by_args(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]]) -> List[AbstractDataTable]:
        return self._data_store.load_by_meta_data(meta_filter)

    @staticmethod
    def _apply_filter(dt: AbstractDataTable, columns: Union[List[str], None] = None,
                      row_filter: Union[List[Tuple[str, Callable]], None] = None) -> pd.DataFrame:
        if not row_filter and not columns:
            return dt.frame
        elif not row_filter and columns:
            return data_aggregation.select_columns(dt, columns)
        elif row_filter and not columns:
            row_mask = data_aggregation.create_mask(dt.frame, row_filter)
            return data_aggregation.filter_rows(dt, row_mask)
        else:
            row_mask = data_aggregation.create_mask(dt.frame, row_filter)
            return data_aggregation.select_columns_rows(dt, columns, row_mask)

    def _apply_filter_full_return(self, dt: AbstractDataTable, columns: Union[List[str], None] = None,
                                  row_filter: Union[List[Tuple[str, Callable]], None] = None) \
            -> Tuple[pd.DataFrame, Dict[str, Any]]:
        return self._apply_filter(dt, columns, row_filter), dt.meta_data
