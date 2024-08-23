"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional

from dataclasses_json import config

from .column_dto import ColumnDTO
from ..audit_dto import AuditDTO
from gravitino.api import Table, Transform, SortOrder, Distribution, Index, Audit
from gravitino.exceptions import (
    IllegalArugmentException,
    IllegalNameIdentifierException,
)


@dataclass
class TableDTO(Table):
    """Represents a Table Data Transfer Object."""

    """
    Represents a Table Data Transfer Object.
    """

    _name: str = field(metadata=config(field_name="name"))
    """The name of the table."""

    _audit: Optional[AuditDTO] = field(metadata=config(field_name="audit"))
    """The audit information of the table."""

    _distribution: Optional[Distribution] = field(
        metadata=config(field_name="distribution")
    )
    """The distribution of the table."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment associated with the table."""

    _columns: List[ColumnDTO] = field(
        metadata=config(field_name="columns"), default_factory=list
    )
    """The columns of the table."""

    _properties: Dict[str, str] = field(
        metadata=config(field_name="properties"), default_factory=dict
    )
    """The properties of the table."""

    _partitioning: List[Transform] = field(
        metadata=config(field_name="partitioning"), default_factory=list
    )
    """The partitioning of the table."""

    _sort_orders: List[SortOrder] = field(
        metadata=config(field_name="sortOrders"), default_factory=list
    )
    """The sort orders of the table."""

    _indexes: List[Index] = field(
        metadata=config(field_name="indexes"), default_factory=list
    )
    """The indexes of the table."""

    def name(self) -> str:
        return self._name

    def columns(self) -> List[ColumnDTO]:
        return self._columns

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit

    def __post_init__(self):
        """Post-initialization to validate the object's data."""
        self.validate()

    def validate(self):
        """Validates the TableDTO fields."""
        if not self._name:
            raise IllegalNameIdentifierException("Table name cannot be null or empty.")
        if not self._columns:
            raise IllegalArugmentException("Table columns cannot be null or empty.")
        if not self._audit:
            raise IllegalArugmentException("Audit cannot be null.")

    @staticmethod
    def builder():
        """Returns a new Builder instance for constructing TableDTO."""
        return Builder()


class Builder:
    """Builder class for constructing TableDTO objects."""

    def __init__(self):
        """Initializes the builder with default values."""
        self._name = None
        self._comment = None
        self._columns = []
        self._properties = {}
        self._audit = None
        self._partitioning = []
        self._distribution = None
        self._sort_orders = []
        self._indexes = []

    def with_name(self, name: str):
        """Sets the name of the table."""
        self._name = name
        return self

    def with_comment(self, comment: Optional[str]):
        """Sets the comment of the table."""
        self._comment = comment
        return self

    def with_columns(self, columns: List[ColumnDTO]):
        """Sets the columns of the table."""
        self._columns = columns
        return self

    def with_properties(self, properties: Dict[str, str]):
        """Sets the properties of the table."""
        self._properties = properties
        return self

    def with_audit(self, audit: Audit):
        """Sets the audit of the table."""
        self._audit = audit
        return self

    def with_partitioning(self, partitioning: List[Transform]):
        """Sets the partitioning of the table."""
        self._partitioning = partitioning
        return self

    def with_distribution(self, distribution: Distribution):
        """Sets the distribution of the table."""
        self._distribution = distribution
        return self

    def with_sort_orders(self, sort_orders: List[SortOrder]):
        """Sets the sort orders of the table."""
        self._sort_orders = sort_orders
        return self

    def with_indexes(self, indexes: List[Index]):
        """Sets the indexes of the table."""
        self._indexes = indexes
        return self

    def build(self) -> TableDTO:
        """Builds and returns a TableDTO object."""
        if not self._name:
            raise IllegalNameIdentifierException("Table name cannot be null or empty.")
        if not self._columns:
            raise IllegalArugmentException("Table columns cannot be null or empty.")
        if not self._audit:
            raise IllegalArugmentException("Audit cannot be null.")

        return TableDTO(
            _name=self._name,
            _comment=self._comment,
            _columns=self._columns,
            _properties=self._properties,
            _audit=self._audit,
            _partitioning=self._partitioning,
            _distribution=self._distribution,
            _sort_orders=self._sort_orders,
            _indexes=self._indexes,
        )
