from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class AbstractPreprocessor(ABC):

    @abstractmethod
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def preprocess_batch_ordered(self, data: List[pd.DataFrame]) -> List[pd.DataFrame]:
        # This method has to conserve order from input to output!
        raise NotImplementedError()

    @abstractmethod
    def update_settings(self, **settings):
        raise NotImplementedError()
