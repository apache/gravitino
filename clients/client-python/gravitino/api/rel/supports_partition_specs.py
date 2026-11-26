# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC, abstractmethod

from gravitino.api.rel.expressions.transforms.transform import Transform


class SupportsPartitionSpecs(ABC):
    """Tables that expose every registered partition spec, not only the current default.

    Some table formats keep historical specs after partition evolution (for example Apache
    Iceberg). Maintenance operations such as rewrite manifests select a ``spec_id``; callers use
    this capability to list valid IDs and their transforms.
    """

    @abstractmethod
    def default_partition_spec_id(self) -> int:
        """Returns the current default partition spec ID for the table.

        Returns:
            int: The default spec ID.
        """

    @abstractmethod
    def partition_specs(self) -> dict[int, list[Transform]]:
        """Returns every partition spec registered on the table, keyed by ``spec-id``.

        Each value is a snapshot of the Gravitino transform list for that spec. The returned map
        should be treated as read-only. The map is never empty for formats that always retain a
        default unpartitioned spec (such as Iceberg) once the table has been loaded with specs.

        Returns:
            dict[int, list[Transform]]: Map from spec ID to transforms.
        """
