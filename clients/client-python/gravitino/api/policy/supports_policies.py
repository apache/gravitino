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

from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_list import PolicyList


class SupportsPolicies(ABC):
    """A metadata object that can list the policies associated with it."""

    @abstractmethod
    def list_policies(self) -> list[str]:
        """Lists the names of the policies associated with this object.

        Returns:
            list[str]: The names of the associated policies.
        """
        raise NotImplementedError()

    def list_policies_with_visibility(self) -> PolicyList[str]:
        """Lists the names of the policies associated with this object,
        together with whether some policies are inaccessible.

        Returns:
            PolicyList[str]: The policy names and the visibility flag.
        """
        return PolicyList(self.list_policies(), False)

    @abstractmethod
    def list_policy_infos(self) -> list[Policy]:
        """Lists the policies associated with this object with their details.

        Returns:
            list[Policy]: The associated policies.
        """
        raise NotImplementedError()

    def list_policy_infos_with_visibility(self) -> PolicyList[Policy]:
        """Lists the policies associated with this object with their details,
        together with whether some policies are inaccessible.

        Returns:
            PolicyList[Policy]: The policies and the visibility flag.
        """
        return PolicyList(self.list_policy_infos(), False)
