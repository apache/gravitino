from abc import ABC, abstractmethod

from mcp_server.connector.catalog_operation import CatalogOperation
from mcp_server.connector.schema_operation import SchemaOperation
from mcp_server.connector.table_operation import TableOperation


class GravitinoOperation(ABC):

    @abstractmethod
    def as_table_operation(self) -> TableOperation:
        pass

    @abstractmethod
    def as_schema_operation(self) -> SchemaOperation:
        pass

    @abstractmethod
    def as_catalog_operation(self) -> CatalogOperation:
        pass
