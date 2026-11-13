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


class DomainOperation(ABC):
    """Read-only domain operations."""

    @abstractmethod
    async def list_of_domains(self, details: bool = True) -> str:
        """List domains; details includes backingTag, status, parent and audit."""

    @abstractmethod
    async def get_domain_by_name(self, domain_name: str) -> str:
        """Load a domain snapshot by name."""

    @abstractmethod
    async def list_subdomains(self, domain_name: str, details: bool = True) -> str:
        """List immediate subdomains, as names or complete snapshots."""

    @abstractmethod
    async def list_domain_members(
        self,
        domain_name: str,
        types: list[str] | None = None,
        domains: list[str] | None = None,
        name_contains: str | None = None,
        expand: bool = False,
    ) -> str:
        """List domain members and all applicable relations.

        By default, lists objects that carry the domain's backing tag themselves,
        including catalogs and schemas; a CATALOG or SCHEMA member includes all
        objects under it and is listed once. expand=true instead expands catalogs
        and schemas into the recorded leaf objects under them, with INHERITED
        relations; use it to enumerate every table, view, fileset, topic, model or
        semantic model in a domain.
        types filters object types (for example TABLE, TOPIC, CATALOG or SCHEMA;
        CATALOG and SCHEMA are not allowed with expand=true).
        domains filters source domain names. name_contains matches a case-insensitive
        substring of the full metadata object name. Blank values omit this filter.
        """

    @abstractmethod
    async def list_domains_for_metadata(
        self, metadata_full_name: str, metadata_type: str, details: bool = True
    ) -> str:
        """List domains for a leaf metadata object.

        metadata_type supports table, fileset, topic, model, view and semantic_model.
        metadata_full_name is the qualified object name, such as catalog.schema.table.
        details preserves all relations and their sources; false returns names only.
        """
