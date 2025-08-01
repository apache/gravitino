from abc import ABC, abstractmethod


class SchemaOperation(ABC):

    @abstractmethod
    def get_list_of_schemas(self, catalog_name: str) -> str:
        pass
