import pickle
from typing import Dict, Any


class AbstractModel:

    def __init__(self, learn_strategy, response_strategy, model_config):
        self.config = model_config
        self._learn_strategy = learn_strategy
        self._response_strategy = response_strategy

    def predict(self, signal: Dict[str, Any], **kwargs):
        return self._response_strategy.predict(self, signal, **kwargs)

    def learn(self, training_signal: Dict[str, Any], **kwargs):
        # In the strategy pattern we will not simply call a function here,
        # but actually call a learn function on a learner object in our model
        return self._learn_strategy.learn(self, training_signal, **kwargs)

    def save(self, file_path):
        with open(file_path, "wb") as output_file:
            pickle.dump(obj=self, file=output_file, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, file_path):
        with open(file_path, "rb") as input_file:
            return pickle.load(input_file)
