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

from pydantic import BaseModel


class ArticleAsset(BaseModel):
    """An asset's type, ordered namespace components and literal leaf name."""

    type: str
    namespace: list[str]
    name: str


class KBArticleOperation(ABC):
    """Read published Knowledge Base Articles in the selected metalake."""

    @abstractmethod
    async def search_kb_articles(
        self,
        q: str | None = None,
        domain: str | None = None,
        owner: str | None = None,
        *,
        limit: int = 20,
        offset: int = 0,
        term: str | None = None,
        asset: ArticleAsset | None = None,
    ) -> str:
        """Return a page of published Article summaries matching the filters."""

    @abstractmethod
    async def get_kb_article(
        self, name: str, limit: int = 20, offset: int = 0
    ) -> str:
        """Read one published revision with its body and binding previews."""

    @abstractmethod
    async def list_kb_articles_for_term(
        self, term: str, limit: int = 20, offset: int = 0
    ) -> str:
        """Return a page of published Articles directly bound to the Term."""

    @abstractmethod
    async def list_kb_articles_for_object(
        self,
        object_type: str,
        namespace: list[str],
        name: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> str:
        """Find published Articles directly bound to a structured asset."""
