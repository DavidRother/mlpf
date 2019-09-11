from .lib.data_table import AbstractDataTable
import logging
from typing import Dict, List, Any, Union, Tuple


class DataStore:

    def __init__(self):
        self._tables: Dict[str, AbstractDataTable] = {}
        self._sources: Dict[str, str] = {}

    def add_source(self, table_id: str, data_source: str, meta_data_keys: Union[List[str], None]):
        """
        Add a table from a data source

        :param table_id:
        :param data_source:
        :param meta_data_keys:
        :return:
        """
        if not self._check_table_id(table_id, data_source):
            return
        self._tables[table_id] = AbstractDataTable.from_source(data_source, meta_data_keys)
        self._sources[table_id] = data_source

    def add_table(self, table_id: str, data_source: str, data_table: AbstractDataTable):
        """
        Add a data table to the data store

        :param table_id:
        :param data_source:
        :param data_table:
        :return:
        """
        if not self._check_table_id(table_id, data_source):
            return
        self._tables[table_id] = data_table
        self._sources[table_id] = data_source

    def add_meta_data(self, table_id: str, meta_data: Tuple[str, Any]):
        """
        Enrich a Data table with meta data

        :param table_id:
        :param meta_data:
        :return:
        """
        self._tables[table_id].add_meta_data_key(meta_data[0], meta_data[1])

    def load_by_id(self, table_id: str) -> AbstractDataTable:
        """
        Get a table by its id

        :param table_id:
        :return:
        """
        return self._tables[table_id]

    def load_by_meta_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]]) -> List[AbstractDataTable]:
        """
        Load all data tables that match the meta data filter
        
        :param meta_filter:
        :return:
        """
        return [dt for dt in self._tables.values() if dt.compare_meta_data(meta_filter[0], meta_filter[1])]

    def _check_table_id(self, table_id, data_source):
        if table_id in self._tables:
            logging.warning('Table id {} is already in use. Skipping import of Data.'.format(table_id))
            return False
        if table_id in self._sources and data_source != self._sources[table_id]:
            logging.warning('Table id {} is already registered with another data source. \nOverwriting old data source '
                            'located at: {} \nwith new data source '
                            'located at: {}'.format(table_id, self._sources[table_id], data_source))
        return True



