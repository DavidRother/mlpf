from abc import ABC, abstractmethod
from ...ModelPackage.abstract_model import AbstractModel

from typing import Dict, Any


class AbstractResponseStrategy(ABC):

    @abstractmethod
    def fit(self, model: AbstractModel, signal: Dict[str, Any], **kwargs):
        raise NotImplementedError()
