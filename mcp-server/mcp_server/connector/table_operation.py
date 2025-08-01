from abc import ABC, abstractmethod


class TableOperation(ABC):

    @abstractmethod
    def get_list_of_tables(self, catalog_name: str, schema_name: str) -> str:
        pass

    @abstractmethod
    def load_table(self, catalog_name: str, schema_name: str, table_name: str) -> str:
        pass
