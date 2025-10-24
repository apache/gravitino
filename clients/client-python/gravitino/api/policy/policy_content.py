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

from gravitino.api.metadata_object import MetadataObject
from gravitino.utils.precondition import Precondition


class PolicyContent(ABC):
    """The interface of the content of the policy."""

    @abstractmethod
    def supported_object_types(self) -> set[MetadataObject.Type]:
        """Returns the set of metadata object types that the policy can be applied to."""

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """Returns the additional properties of the policy."""

    def validate(self) -> None:
        """Validates the policy content.

        Raises:
            IllegalArgumentException: if the content is invalid.
        """
        Precondition.check_argument(
            len(self.supported_object_types()) > 0,
            "supported_object_types cannot be empty",
        )
