from abc import ABC, abstractmethod


class CatalogOperation(ABC):

    @abstractmethod
    def get_list_of_catalogs(self) -> str:
        pass
