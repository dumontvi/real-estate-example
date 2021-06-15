from abc import ABC
from abc import abstractmethod


class Extract(ABC):
    @abstractmethod
    def extract(self):
        pass


class Transform(ABC):
    @abstractmethod
    def transform(self):
        pass


class Loader(ABC):
    @abstractmethod
    def load(self):
        pass
