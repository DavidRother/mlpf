from abc import ABC, abstractmethod
from ...ModelPackage.abstract_model import AbstractModel

from typing import Dict, Any


class AbstractLearningStrategy(ABC):

    @abstractmethod
    def fit_transform(self, model: AbstractModel, training_signal: Dict[str, Any], **kwargs):
        raise NotImplementedError()
