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

from __future__ import annotations

from abc import ABC, abstractmethod


class SupportsRoles(ABC):
    """Interface for supporting list role names for objects. This interface will be mixed with metadata
    objects to provide listing role operations.
    """

    @abstractmethod
    def list_binding_role_names(self) -> list[str]:
        """
        List all the role names associated with this metadata object.

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            list[str]: The role name list associated with this metadata object.
        """
        raise NotImplementedError()
