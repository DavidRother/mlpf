from ..DataWarehousePackage.data_warehouse import DataWarehouse
from ..ModelPackage.abstract_model import AbstractModel
from ..ModelPackage import model_factory
from ..BackendPackage.lib import learning_plans

from itertools import zip_longest
from typing import Dict, List, Tuple, Union, Callable, Any, Optional

import pickle
import pandas as pd
import uuid
import json
import logging


class SystemManager:

    def __init__(self, sys_name, config_path):
        with open(config_path) as json_file:
            self.config = json.load(json_file)
        self.model_data_pairs: Dict[str, List[Union[AbstractModel, None], DataWarehouse]] = {}
        self.config_path = config_path
        self.sys_name = sys_name

    @staticmethod
    def load(filename: str):
        """
        Load an existing system by checking if the folder exists in the workbench and then loading the
        config files and the pickled models.

        :param filename:
        :return:
        """
        with open(filename, 'rb') as in_file:
            system = pickle.load(in_file)
            with open(system.config_path) as json_file:
                system.config = json.load(json_file)
            return system

    def save(self, filename: str):
        """
        Save the current system to the specified location.

        :param filename:
        :return:
        """
        with open(filename, 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def create_new_model_data_instance(self, model_name, model_config, data_root: str) -> str:
        """
        Create a model and data instance if they are provided and add them as new model data pair to the system.
        Return the corresponding model data ID.

        :param model_name:
        :param model_config:
        :param data_root:
        :return:
        """
        if model_name:
            new_model = model_factory.create_model(model_name, settings=model_config)
        else:
            new_model = None
        new_warehouse = DataWarehouse(data_root)
        new_id = str(uuid.uuid4())
        self.model_data_pairs[new_id] = [new_model, new_warehouse]
        return new_id

    def load_data(self, model_id: str, folders: List[str], meta_data_keys: Union[List[str], None]) -> List[str]:
        """
        Load the data of the data warehouse into memory.

        :param model_id:
        :param folders:
        :param meta_data_keys:
        :return:
        """
        _, warehouse = self.model_data_pairs[model_id]
        return warehouse.load_data_folders(folders, meta_data_keys)

    def learn_data(self, model_id: str, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]],
                   columns: Union[List[str], None] = None, row_filter: Union[List[Tuple[str, Callable]], None] = None,
                   granularity: int = 1, learning_plan: Tuple[str, Optional[Dict]] = ('random', {'seed': 1}), **kwargs):
        """
        Perform Learning using the filtered data on the model specified by the model data pair ID.

        :param model_id:
        :param meta_filter:
        :param columns:
        :param row_filter:
        :param granularity:
        :param learning_plan:
        :param kwargs:
        :return:
        """
        logging.debug('Starting online learning')
        model, warehouse = self.model_data_pairs[model_id]
        data_frames = warehouse.get_complete_data_by_meta_data(meta_filter, columns, row_filter)
        data_frames = learning_plans.get_data_plan(learning_plan[0])(data_frames, **learning_plan[1])
        for feed_frames in zip_longest(fillvalue=None, *[iter(data_frames)] * granularity):
            training_signal = self._build_training_signal(feed_frames)
            yield model.learn(training_signal, **kwargs)

    def add_model_to_data_source(self, model_id, model_config, model_type):
        """
        Create a new model instance and add it to the model data pair.

        :param model_id:
        :param model_config:
        :param model_type:
        :return:
        """
        self.model_data_pairs[model_id] = [model_factory.create_model(model_type, model_config),
                                           self.model_data_pairs[model_id][1]]

    def add_data_source(self, model_id, data_root, override=True):
        """
        Add a new Data warehouse instance to the data model pair.

        :param model_id:
        :param data_root:
        :param override:
        :return:
        """
        new_warehouse = DataWarehouse(data_root)
        if override:
            self.model_data_pairs[model_id] = [self.model_data_pairs[model_id][0], new_warehouse]

    def get_model(self, model_id: str) -> AbstractModel:
        """
        Takes a model id and returns the currently associated model

        :param model_id:
        :return:
        """
        return self.model_data_pairs[model_id][0]

    def preprocess(self, model_id: str, method_name: str, column_names: List[str], settings: Dict,
                   mark_new: Tuple[str, Any], mark_old: Union[Tuple[str, Any], None], batch_mode: bool,
                   table_ids: Union[None, List[str]] = None,
                   meta_filter: Union[Tuple[Dict[str, Any], Dict[str, Any]], None] = None) -> List[str]:
        """
        Perform preprocessing algorithms on the specified subset of the data and mark the preprocessed data.

        :param model_id:
        :param method_name:
        :param column_names:
        :param settings:
        :param mark_new:
        :param mark_old:
        :param batch_mode:
        :param table_ids:
        :param meta_filter:
        :return:
        """
        _, warehouse = self.model_data_pairs[model_id]
        if table_ids and meta_filter is not None:
            logging.warning('Please specify either only a list of table ids or meta data. Skipping Preprocessing.')
            return []
        if table_ids:
            return warehouse.preprocessing_by_id(table_ids, method_name, column_names,
                                                 settings, mark_new, mark_old, batch_mode)
        if meta_filter is not None:
            return warehouse.preprocessing_by_args(meta_filter, method_name, column_names,
                                                   settings, mark_new, mark_old, batch_mode)

    def get_data(self, model_id: str, table_id: Optional[str] = None,
                 meta_filter: Optional[Tuple[Dict[str, Any], Dict[str, Any]]] = None,
                 columns: Optional[List[str]] = None,
                 row_filter: Optional[List[Tuple[str, Callable]]] = None) -> List[pd.DataFrame]:
        """
        Retrieve a subset of filtered data.

        :param model_id:
        :param table_id:
        :param meta_filter:
        :param columns:
        :param row_filter:
        :return:
        """
        _, warehouse = self.model_data_pairs[model_id]
        df_list = []
        if table_id is not None:
            df_list.append(warehouse.get_data_by_id(table_id, columns, row_filter))
        if meta_filter is not None:
            df_list.extend(warehouse.get_data_by_meta_data(meta_filter, columns, row_filter))
        return df_list

    def get_complete_data(self, model_id: str, meta_filter: Optional[Tuple[Dict[str, Any], Dict[str, Any]]] = None,
                          columns: Optional[List[str]] = None, row_filter: Optional[List[Tuple[str, Callable]]] = None)\
            -> List[Tuple[pd.DataFrame, Dict[str, Any]]]:
        _, warehouse = self.model_data_pairs[model_id]
        return warehouse.get_complete_data_by_meta_data(meta_filter, columns, row_filter)

    def sync_data_warehouse(self, model_id):
        warehouse = self.model_data_pairs[model_id][1]
        for model_data_pair in self.model_data_pairs.values():
            model_data_pair[1] = warehouse

    @staticmethod
    def _build_training_signal(data_frames: List[Tuple[pd.DataFrame, Dict[str, Any]]]) -> Dict[str, Any]:
        frames = [tup[0] for tup in data_frames]
        meta_data = [tup[1] for tup in data_frames]
        if len(frames) == 1:
            frames = frames[0]
            meta_data = meta_data[0]
        return {'data_signal': frames, 'meta_data': meta_data}

    @staticmethod
    def default_config():
        return {}
