from abc import ABC

from connector.catalog_operation import CatalogOperation
from connector.schema_operation import SchemaOperation
from connector.table_operation import TableOperation


class GravitinoOperation(ABC):

    def as_table_operation(self) -> TableOperation:
        pass

    def as_schema_operation(self) -> SchemaOperation:
        pass

    def as_catalog_operation(self) -> CatalogOperation:
        pass
