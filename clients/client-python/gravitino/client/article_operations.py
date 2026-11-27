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

import json
from typing import Optional

from gravitino.api.kb.article import (
    DEFAULT_LIMIT,
    ArticleContent,
    ArticleCreate,
    ArticleListFilter,
    ArticlePage,
    AssetBindingPage,
    KnowledgeBaseArticle,
    TermBindingPage,
    validate_pagination,
)
from gravitino.api.kb.article_change import KnowledgeBaseArticleChange
from gravitino.api.kb.article_operations import KnowledgeBaseArticleOperations
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.exceptions.handlers.article_error_handler import ARTICLE_ERROR_HANDLER
from gravitino.rest.rest_utils import encode_domain_path_segment, encode_string
from gravitino.utils.article_name import normalize_article_name
from gravitino.utils.http_client import HTTPClient, Response
from gravitino.utils.precondition import Precondition


class ArticleOperations(KnowledgeBaseArticleOperations):
    """REST-backed article operations for one metalake."""

    def __init__(self, metalake: str, client: HTTPClient):
        self._path = f"api/metalakes/{encode_string(metalake)}/kb-articles"
        self._client = client

    def list_articles(
        self, article_filter: ArticleListFilter = ArticleListFilter()
    ) -> ArticlePage:
        Precondition.check_argument(
            isinstance(article_filter, ArticleListFilter),
            "An article list filter is required",
        )
        owner = article_filter.owner
        params = {
            "limit": article_filter.limit,
            "offset": article_filter.offset,
            "q": article_filter.query,
            "domain": article_filter.domain,
            "status": article_filter.status.name if article_filter.status else None,
            "owner": f"{owner.type().name.lower()}:{owner.name()}" if owner else None,
            "term": article_filter.term,
            "includeDeprecated": "true" if article_filter.include_deprecated else None,
        }
        obj = article_filter.object
        if obj is not None:
            # HTTPClient encodes a sequence as repeated keys and preserves its order.
            params.update(
                objectType=obj.type.name, namespace=obj.namespace, name=obj.name
            )
        response = self._client.get(
            self._path, params=params, error_handler=ARTICLE_ERROR_HANDLER
        )
        return ArticlePage.from_dict(self._payload(response))

    def load_article(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> KnowledgeBaseArticle:
        response = self._client.get(
            self._article_path(name),
            params=self._pagination(limit, offset),
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        return KnowledgeBaseArticle.from_dict(self._payload(response)["article"])

    def get_article_content(self, name: str) -> ArticleContent:
        response = self._client.get(
            f"{self._article_path(name)}/content", error_handler=ARTICLE_ERROR_HANDLER
        )
        return ArticleContent.from_dict(self._payload(response))

    def create_article(self, article_create: ArticleCreate) -> KnowledgeBaseArticle:
        Precondition.check_argument(
            isinstance(article_create, ArticleCreate),
            "Article creation fields are required",
        )
        response = self._client.post(
            self._path,
            data=json.dumps(article_create.to_dict()),
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        return KnowledgeBaseArticle.from_dict(self._payload(response)["article"])

    def alter_article(
        self,
        name: str,
        *changes: KnowledgeBaseArticleChange,
        expected_revision: Optional[str] = None,
    ) -> KnowledgeBaseArticle:
        Precondition.check_argument(
            bool(changes)
            and all(
                isinstance(change, KnowledgeBaseArticleChange) for change in changes
            ),
            "Updates must contain one or more article changes",
        )
        request = {"updates": [change.to_dict() for change in changes]}
        if expected_revision is not None:
            Precondition.check_argument(
                isinstance(expected_revision, str), "Expected revision must be a string"
            )
            request["expectedRevision"] = expected_revision
        response = self._client.put(
            self._article_path(name),
            data=json.dumps(request),
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        return KnowledgeBaseArticle.from_dict(self._payload(response)["article"])

    def delete_article(self, name: str) -> bool:
        response = self._client.delete(
            self._article_path(name), error_handler=ARTICLE_ERROR_HANDLER
        )
        result = DropResponse.from_json(response.body)
        result.validate()
        return result.dropped()

    def detach_domain(self, name: str, domain: str) -> None:
        response = self._client.delete(
            f"{self._article_path(name)}/domains/{encode_domain_path_segment(domain)}",
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        BaseResponse.from_json(response.body).validate()

    def list_asset_bindings(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> AssetBindingPage:
        response = self._client.get(
            f"{self._article_path(name)}/objects",
            params=self._pagination(limit, offset),
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        return AssetBindingPage.from_dict(self._payload(response))

    def list_term_bindings(
        self, name: str, limit: int = DEFAULT_LIMIT, offset: int = 0
    ) -> TermBindingPage:
        response = self._client.get(
            f"{self._article_path(name)}/terms",
            params=self._pagination(limit, offset),
            error_handler=ARTICLE_ERROR_HANDLER,
        )
        return TermBindingPage.from_dict(self._payload(response))

    def _article_path(self, name: str) -> str:
        return f"{self._path}/{encode_string(normalize_article_name(name))}"

    @staticmethod
    def _pagination(limit: int, offset: int) -> dict:
        validate_pagination(limit, offset)
        return {"limit": limit, "offset": offset}

    @staticmethod
    def _payload(response: Response) -> dict:
        payload = json.loads(response.body)
        BaseResponse.from_dict(payload).validate()
        return payload
