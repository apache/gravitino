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

from gravitino.api.domain.domain import Domain


class SupportsDomains(ABC):
    """Domain lookup for a leaf object. Membership does not grant data access."""

    @abstractmethod
    def list_domains(self) -> list[str]:
        """List visible domains whose scopes contain this object."""
        raise NotImplementedError

    @abstractmethod
    def list_domains_info(self) -> list[Domain]:
        """List snapshots with all applicable relations, without precedence."""
        raise NotImplementedError
