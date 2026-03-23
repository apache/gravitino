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

import typing as tp
from abc import ABC, abstractmethod

from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import UnsupportedOperationException
from gravitino.utils.precondition import Precondition


class PolicyContent(ABC):
    """
    The interface of the content of the policy.
    """

    @abstractmethod
    def supported_object_types(self) -> set[MetadataObject.Type]:
        """
        Return the set of metadata object types that the policy can be applied to

        Raises:
            NotImplementedError: If the method is not implemented.

        Returns:
            set[MetadataObject.Type]: the set of metadata object types that the policy can be applied to
        """
        raise NotImplementedError()

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """
        Return the additional properties of the policy.

        Raises:
            NotADirectoryError: If the method is not implemented.

        Returns:
            dict[str, str]: The additional properties of the policy.
        """
        raise NotADirectoryError()

    def rules(self) -> dict[str, tp.Any]:
        """
        A convenience method to get all rules in the policy content.

        Raises:
            UnsupportedOperationException: If it doesn't support get all rules.

        Returns:
            dict[str, tp.Any]: A map of rule names to their corresponding rule objects.
        """
        raise UnsupportedOperationException("Does support get all rules.")

    def validate(self) -> None:
        """
        Validates the policy content.
        """
        support_types = self.supported_object_types()
        Precondition.check_argument(
            support_types is not None and len(support_types) > 0,
            "The supported object types of the policy cannot be empty.",
        )
