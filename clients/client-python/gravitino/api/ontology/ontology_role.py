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
from typing import Optional


class OntologyRole(ABC):
    """A concept playing a role in an ontology relationship.

    The role name is needed only when the same concept plays more than one role
    in the same relationship. Otherwise the concept alone identifies the role.
    """

    @abstractmethod
    def concept(self) -> str:
        """The name of the concept playing this role.

        Returns:
            str: The concept name.
        """
        pass

    @abstractmethod
    def name(self) -> Optional[str]:
        """The optional role name.

        Returns:
            Optional[str]: The role name, or `None` if the concept alone
                identifies the role.
        """
        pass
