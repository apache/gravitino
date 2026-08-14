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

from abc import abstractmethod
from typing import Optional, Dict

from gravitino.api.auditable import Auditable
from gravitino.api.authorization.supports_roles import SupportsRoles
from gravitino.exceptions.base import UnsupportedOperationException


class Metalake(Auditable):
    """
    The interface of a metalake. The metalake is the top level entity in the gravitino system,
    containing a set of catalogs.
    """

    @abstractmethod
    def name(self) -> str:
        """The name of the metalake.

        Returns:
            str: The name of the metalake.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the metalake. Note. this method will return None if the comment is not set for
        this metalake.

        Returns:
            Optional[str]: The comment of the metalake.
        """
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        """The properties of the metalake. Note, this method will return None if the properties are not
        set.

        Returns:
            Optional[Dict[str, str]]: The properties of the metalake.
        """
        pass

    def supports_roles(self) -> SupportsRoles:
        """Return role operations supported by this metalake.

        Returns:
            SupportsRoles: The role operations supported by this metalake.

        Raises:
            UnsupportedOperationException: If this metalake does not support role operations.
        """
        raise UnsupportedOperationException(
            "Metalake does not support role operations."
        )
