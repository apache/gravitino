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
from typing import Optional

from gravitino.api.kb.article import (
    DEFAULT_LIMIT,
    ArticleContent,
    ArticleCreate,
    ArticleListFilter,
    ArticleObjectIdentifier,
    ArticlePage,
    AssetBindingPage,
    KnowledgeBaseArticle,
    TermBindingPage,
)
from gravitino.api.kb.article_change import KnowledgeBaseArticleChange
from gravitino.utils.precondition import Precondition


class KnowledgeBaseArticleOperations(ABC):
    """Manage Knowledge Base Articles and their bindings within a metalake."""

    @abstractmethod
    def list_articles(
        self, article_filter: ArticleListFilter = ArticleListFilter()
    ) -> ArticlePage:
        """Get one page of visible articles matching all supplied filters."""

    @abstractmethod
    def load_article(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> KnowledgeBaseArticle:
        """Load article metadata with paginated previews of visible assets and terms."""

    @abstractmethod
    def get_article_content(self, name: str) -> ArticleContent:
        """Read the current body and revision token together."""

    @abstractmethod
    def create_article(self, article_create: ArticleCreate) -> KnowledgeBaseArticle:
        """Create an article with optional initial domain, term and asset bindings."""

    @abstractmethod
    def alter_article(
        self,
        name: str,
        *changes: KnowledgeBaseArticleChange,
        expected_revision: Optional[str] = None,
    ) -> KnowledgeBaseArticle:
        """Apply changes atomically in order.

        Supply the revision token from detail or content to reject a stale write
        with ArticleRevisionConflictException. Omit it for an unconditional write.
        """

    @abstractmethod
    def delete_article(self, name: str) -> bool:
        """Return True on deletion and False for an absent, deleted or hidden article."""

    @abstractmethod
    def detach_domain(self, name: str, domain: str) -> None:
        """Detach one domain from the article."""

    @abstractmethod
    def list_asset_bindings(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> AssetBindingPage:
        """List a page of bound metadata objects readable by the caller."""

    @abstractmethod
    def list_term_bindings(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> TermBindingPage:
        """List a page of bound terms readable by the caller."""

    def list_articles_for_term(
        self,
        term: str,
        include_deprecated: bool = False,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
    ) -> ArticlePage:
        """List published articles bound to the term, optionally including deprecated articles."""
        Precondition.check_argument(
            isinstance(term, str) and bool(term.strip()), "Term must not be blank"
        )
        return self.list_articles(
            ArticleListFilter(
                term=term,
                include_deprecated=include_deprecated,
                limit=limit,
                offset=offset,
            )
        )

    def list_articles_for_object(
        self,
        obj: ArticleObjectIdentifier,
        include_deprecated: bool = False,
        limit: int = DEFAULT_LIMIT,
        offset: int = 0,
    ) -> ArticlePage:
        """List published articles bound to the asset, optionally including deprecated articles."""
        Precondition.check_argument(
            isinstance(obj, ArticleObjectIdentifier), "An object identifier is required"
        )
        return self.list_articles(
            ArticleListFilter(
                object=obj,
                include_deprecated=include_deprecated,
                limit=limit,
                offset=offset,
            )
        )
