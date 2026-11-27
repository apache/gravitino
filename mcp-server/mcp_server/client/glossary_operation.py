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


class GlossaryOperation(ABC):
    """Read approved glossary terms and their visible bindings."""

    @abstractmethod
    async def resolve_business_term(
        self,
        words: list[str],
        domain: str | None = None,
        include_bindings: bool = True,
    ) -> str:
        """Resolve business phrases against term names and synonyms."""

    @abstractmethod
    async def get_term(self, term: str) -> str:
        """Load a term definition and its visible relations."""

    @abstractmethod
    async def list_terms(
        self, domain: str | None = None, name_contains: str | None = None
    ) -> str:
        """Browse term definitions, optionally by domain or name fragment."""

    @abstractmethod
    async def get_term_hierarchy(
        self, term: str, direction: str = "NARROWER"
    ) -> str:
        """List reachable terms in the requested direction."""

    @abstractmethod
    async def list_metadata_by_term(
        self, term: str, kind: str | None = None, types: list[str] | None = None
    ) -> str:
        """List the objects and metrics bound to a term."""

    @abstractmethod
    async def list_terms_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        """List direct terms and, for tables or semantic models, child terms."""
