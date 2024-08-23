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

from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.exceptions.base import IllegalArugmentException

@dataclass
class TableCreateRequest(RESTRequest):
    name: str
    columns: List[ColumnDTO] = field(default_factory=list, metadata=config(field_name="columns"))
    properties: Dict[str, str] = field(default_factory=dict, metadata=config(field_name="properties"))
    comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    sort_orders: Optional[List["SortOrderDTO"]] = field(default=None, metadata=config(field_name="sortOrders"))
    distribution: Optional["DistributionDTO"] = field(default=None, metadata=config(field_name="distribution"))
    partitioning: Optional[List["Partitioning"]] = field(default=None, metadata=config(field_name="partitioning"))
    indexes: Optional[List["IndexDTO"]] = field(default=None, metadata=config(field_name="indexes"))

    def __post_init__(self):
        self.validate()

    def validate(self):
        if not self.name.strip():
            raise IllegalArugmentException("\"name\" field is required and cannot be empty")
        if not self.columns:
            raise IllegalArugmentException("\"columns\" field is required and cannot be empty")

        if self.sort_orders:
            NotImplementedError("Sort orders are not yet supported")

        if self.distribution:
            NotImplementedError("Distribution is not yet supported")

        if self.partitioning:
            NotImplementedError("Partitioning is not yet supported")

        if self.indexes:
            NotImplementedError("Indexes are not yet supported")

        auto_increment_cols = [column for column in self.columns if column.auto_increment()]
        if len(auto_increment_cols) > 1:
            auto_increment_cols_str = ", ".join([col.name for col in auto_increment_cols])
            raise IllegalArugmentException(f"Only one column can be auto-incremented. There are multiple auto-increment columns in your table: [{auto_increment_cols_str}]")
