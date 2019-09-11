from .system_manager import SystemManager
from typing import List, Dict, Any, Tuple
from abc import ABC, abstractmethod

import pandas as pd
import logging
import json
import os
import shutil
import tkinter

from tkinter import messagebox


class AbstractBackend(ABC):

    HUMAN_TRAJECTORY_X = ' human_x'
    HUMAN_TRAJECTORY_Y = 'human_y'
    HUMAN_TRAJECTORY_Z = 'human_z'
    TIME = 't'

    def __init__(self, workbench_path: str, data_root: str, config=None):
        self.system: [SystemManager, None] = None
        self.workbench_path: str = workbench_path
        self.data_root: str = data_root
        self.active_model: str = ''
        self.model_ids = []
        self.__version__ = 0.1
        self.config = config or self._default_config()

    def create_new_system(self, system_name: str) -> str:
        """
        Create a new system and ask if the old system should be overridden (if it exists)

        :param system_name:
        :return:
        """
        sys_path = self._check_workbench(system_name)
        self.system: SystemManager = SystemManager(system_name, os.path.join(sys_path, 'Config',
                                                                             'sys_manager_config.json'))
        self.model_ids = []
        self.active_model = self.system.create_new_model_data_instance('', {}, self.data_root)
        self.model_ids.append(self.active_model)
        return self.active_model

    def get_custom_field_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]], columns: List[str]) \
            -> List[pd.DataFrame]:
        """
        Retrieve the custom fields from the data that match the filter

        :param meta_filter:
        :param columns:
        :return:
        """
        return self.system.get_data(self.active_model, meta_filter=meta_filter, columns=columns)

    def get_complete_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]], columns: List[str]) \
            -> List[pd.DataFrame]:
        """
        Retrieve the meta field data

        :param meta_filter:
        :param columns:
        :return:
        """
        return self.system.get_complete_data(self.active_model, meta_filter=meta_filter, columns=columns)

    def get_human_trajectories(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]]) -> List[pd.DataFrame]:
        """
        Retrieve the human trajectories from the data that match the filter

        :param meta_filter:
        :return:
        """
        return self.get_custom_field_data(meta_filter=meta_filter,
                                          columns=[self.HUMAN_TRAJECTORY_X, self.HUMAN_TRAJECTORY_Y,
                                                   self.HUMAN_TRAJECTORY_Z])

    def preprocess_human_trajectories(self, method: str, settings: Dict, marker_new: Tuple,
                                      meta_filter: Tuple[Dict, Dict] = None, batch_mode=False):
        """
        Preprocess the human trajectories
        
        :param method: definition of the method as defined in preprocessing pkg
        :param settings: the settings of this method, handed over to it
        :param marker_new: the tag that will be marking the processed data in the data ware house table
        :param meta_filter: the tag that was used for the preprocessing before if chaining preprocessing methods
        :param batch_mode: if True the data will be preprocessed as a batch
        :return:
        """
        meta_filter = meta_filter or ({}, {})
        self.system.preprocess(self.active_model, method,
                               column_names=[self.TIME, self.HUMAN_TRAJECTORY_X, self.HUMAN_TRAJECTORY_Y,
                                             self.HUMAN_TRAJECTORY_Z], settings=settings,
                               mark_new=marker_new, mark_old=None, batch_mode=batch_mode,
                               meta_filter=meta_filter)

    def add_model(self, model_name, model_config):
        """
        Add a model to the active system

        :param model_name:
        :param model_config:
        :return:
        """
        self.system.add_model_to_data_source(self.active_model, model_config, model_name)

    def add_data_source(self, data_root):
        """
        Add a data source to the active system

        :param data_root:
        :return:
        """
        self.system.add_data_source(self.active_model, data_root, override=True)

    def add_model_data_system(self, model_name, model_config, data_root):
        """
        Create a new system instance and set it as the active one

        :param model_name:
        :param model_config:
        :param data_root:
        :return:
        """
        model_id = self.system.create_new_model_data_instance(model_name, model_config, data_root)
        if model_id:
            self.model_ids.append(model_id)
            self.active_model = model_id
        return model_id

    def sync_data(self):
        """
        Sync all models to share the same data warehouse instance of the currently active model

        :return:
        """
        self.system.sync_data_warehouse(self.active_model)

    def load_data(self):
        """
        Load the data of the currently active system

        :return:
        """
        self.system.load_data(self.active_model, self.config['subjects'], self.config['meta_data'])

    def set_active_model(self, model_id: str):
        """
        Change the active model

        :param model_id:
        :return:
        """
        self.active_model = model_id

    def learn_online_data(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]],
                          learning_plan: Tuple[str, Any] = ('', {})):
        """
        Perform online learning with the currently active system

        :param meta_filter:
        :param learning_plan:
        :return:
        """
        columns = [self.HUMAN_TRAJECTORY_X, self.HUMAN_TRAJECTORY_Y, self.HUMAN_TRAJECTORY_Z]
        row_filter = None
        granularity = 1
        statistics_gathered = []
        for statistics in self.system.learn_data(self.active_model, meta_filter, columns,
                                                 row_filter, granularity, learning_plan):
            statistics_gathered.append(statistics)
        return statistics_gathered

    def learn_online_data_step(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]],
                               learning_plan: Tuple[str, Any] = ('', {})):
        """
        Perform one step of online learning on the currently active system

        :param meta_filter:
        :param learning_plan:
        :return:
        """

        logging.info('learn data step backend')
        columns = [self.HUMAN_TRAJECTORY_X, self.HUMAN_TRAJECTORY_Y, self.HUMAN_TRAJECTORY_Z]
        row_filter = None
        granularity = 1

        for statistics in self.system.learn_data(self.active_model, meta_filter, columns,
                                                 row_filter, granularity, learning_plan):
            yield statistics

    def amount_data_points(self, meta_filter: Tuple[Dict[str, Any], Dict[str, Any]]):
        """
        Get the amount of data points that match the specified filter

        :param meta_filter:
        :return:
        """
        return len(self.system.get_data(self.active_model, meta_filter=meta_filter))

    def _create_workbench_folder_system(self, system_name: str) -> str:
        new_system_path = os.path.join(self.workbench_path, system_name)
        if os.path.isdir(new_system_path):
            logging.info('Folder with name {} already exists. Please select another name.'.format(system_name))
            return ''
        self._create_directories(new_system_path)
        self._create_configs(new_system_path)
        return new_system_path

    def save(self):
        sys_path = os.path.join(self.workbench_path, self.system.sys_name)
        with open(os.path.join(sys_path, 'SaveState', 'backend_state.json'), 'w') as outfile:
            json.dump(self._state_dict(), outfile, indent=4, sort_keys=True)
        with open(os.path.join(sys_path, 'Config', 'backend_config.json'), 'w') as outfile2:
            json.dump(self.config, outfile2, indent=4, sort_keys=True)
        self.system.save(os.path.join(sys_path, 'SaveState', 'system'))

    def load(self, path):
        self.system = SystemManager.load(os.path.join(path))
        sys_path = os.path.join(self.workbench_path, self.system.sys_name)
        self._restore_state(os.path.join(sys_path, 'SaveState', 'backend_state.json'))
        self._reload_config(os.path.join(sys_path, 'Config', 'backend_state.json'))

    def get_model_ids(self):
        """
        Get all model ids currently known in the system

        :return:
        """
        return self.system.model_data_pairs.keys()

    def get_model(self):
        """
        Get the currently active model instance

        :return:
        """
        return self.system.get_model(self.active_model)

    def _state_dict(self):
        return {'workbench_path': self.workbench_path, 'data_root': self.data_root, 'active_model': self.active_model,
                'version': self.__version__, 'model_ids': self.model_ids}

    def _reload_config(self, path):
        with open(os.path.join(path, 'Config', 'backend_config.json'), 'w') as infile:
            self.config = json.load(infile)

    def _restore_state(self, path):
        with open(path, 'r') as infile:
            state_dict = json.load(infile)
        self.workbench_path = state_dict['workbench_path']
        self.data_root = state_dict['data_root']
        self.active_model = state_dict['active_model']
        self.__version__ = state_dict['version']
        self.model_ids = state_dict['model_ids']

    def _check_workbench(self, system_name):
        sys_path = self._create_workbench_folder_system(system_name)
        if not sys_path:
            root = tkinter.Tk()
            root.overrideredirect(1)
            root.withdraw()
            replace = messagebox.askyesno("Python", "Would you like to replace the current workbench?")
            if replace:
                shutil.rmtree(os.path.join(self.workbench_path, system_name), ignore_errors=True)
                sys_path = self._create_workbench_folder_system(system_name)

            else:
                return ''
        return sys_path

    @staticmethod
    def _create_directories(new_system_path: str):
        os.mkdir(new_system_path)
        os.mkdir(os.path.join(new_system_path, 'Config'))
        os.mkdir(os.path.join(new_system_path, 'ResultTables'))
        os.mkdir(os.path.join(new_system_path, 'Visualizations'))
        os.mkdir(os.path.join(new_system_path, 'SaveState'))

    def _create_configs(self, new_system_path: str):
        with open(os.path.join(new_system_path, 'Config', 'backend_config.json'), 'w') as outfile:
            json.dump(self.config, outfile, indent=4, sort_keys=True)
        with open(os.path.join(new_system_path, 'Config', 'sys_manager_config.json'), 'w') as outfile2:
            json.dump(SystemManager.default_config(), outfile2, indent=4, sort_keys=True)

    @abstractmethod
    def _default_config(self):
        return {}

