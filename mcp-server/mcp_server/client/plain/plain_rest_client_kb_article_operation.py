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

import simplejson

from mcp_server.client.kb_article_operation import (
    ArticleAsset,
    KBArticleOperation,
)
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.utils import encode_path_segment, response_content


class PlainRESTClientKBArticleOperation(KBArticleOperation):
    """Read Articles through the current principal's REST client."""

    def __init__(self, metalake_name: str | None, rest_client):
        self._metalake_name = metalake_name
        self._rest_client = rest_client

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
        if term is not None and not term.strip():
            raise ValueError("Term name must not be blank")
        params = {
            "status": "PUBLISHED",
            "limit": limit,
            "offset": offset,
            "q": q,
            "domain": domain,
            "owner": owner,
            "term": term,
        }
        if asset is not None:
            # httpx sends each namespace component as its own query parameter.
            params.update(
                objectType=asset.type,
                namespace=asset.namespace,
                name=asset.name,
            )
        page = await self._get("", **params)
        articles = page["articles"]
        if not isinstance(articles, list) or any(
            not isinstance(article, dict)
            or article.get("status") != "PUBLISHED"
            for article in articles
        ):
            raise GravitinoException("Invalid published Article list")
        return simplejson.dumps(
            {"articles": articles, "hasMore": page["hasMore"]}
        )

    async def get_kb_article(
        self, name: str, limit: int = 20, offset: int = 0
    ) -> str:
        path = f"/{encode_path_segment(name)}"
        detail = await self._get(path, limit=limit, offset=offset)
        article = detail["article"]
        if article.get("status") != "PUBLISHED":
            raise GravitinoException("Published Article not found")
        revision = article.get("revisionToken")
        if not isinstance(revision, str) or not revision.strip():
            raise GravitinoException("Article metadata has no revision token")

        content = await self._get(f"{path}/content")
        # Publication status and body come from separate endpoints. A status
        # change, deletion or recreation between reads invalidates the token.
        if content.get("revisionToken") != revision:
            raise GravitinoException("Article changed during the read")
        return simplejson.dumps({**article, "body": content["body"]})

    async def list_kb_articles_for_term(
        self, term: str, limit: int = 20, offset: int = 0
    ) -> str:
        return await self.search_kb_articles(
            term=term, limit=limit, offset=offset
        )

    async def list_kb_articles_for_object(
        self,
        object_type: str,
        namespace: list[str],
        name: str,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> str:
        return await self.search_kb_articles(
            asset=ArticleAsset(
                type=object_type, namespace=namespace, name=name
            ),
            limit=limit,
            offset=offset,
        )

    async def _get(self, path: str, **params) -> dict:
        if not self._metalake_name or not self._metalake_name.strip():
            raise ValueError("Select a metalake before using KB Article tools")
        metalake = encode_path_segment(self._metalake_name)
        response = await self._rest_client.get(
            f"/api/metalakes/{metalake}/kb-articles{path}",
            params={
                key: value for key, value in params.items() if value is not None
            },
        )
        payload = response_content(response)
        if not isinstance(payload, dict) or "code" not in payload:
            raise GravitinoException("Invalid Gravitino response: missing code")
        return payload
