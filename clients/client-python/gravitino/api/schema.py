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

from abc import abstractmethod
from typing import Optional, Dict

from gravitino.api.auditable import Auditable


class Schema(Auditable):
    """
    An interface representing a schema in the Catalog. A Schema is a
    basic container of relational objects, like tables, views, etc. A Schema can be self-nested,
    which means it can be schema1.schema2.table.

    This defines the basic properties of a schema. A catalog implementation with SupportsSchemas
    should implement this interface.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the name of the Schema."""
        pass

    def comment(self) -> Optional[str]:
        """Returns the comment of the Schema. None is returned if the comment is not set."""
        return None

    def properties(self) -> Dict[str, str]:
        """Returns the properties of the Schema. An empty dictionary is returned if no properties are set."""
        return {}
