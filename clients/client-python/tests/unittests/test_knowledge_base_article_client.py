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
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from gravitino.api.authorization.owner import Owner
from gravitino.api.kb import (
    ArticleBody,
    ArticleCreate,
    ArticleListFilter,
    ArticleObjectIdentifier,
    ArticleStatus,
    ArticleTermBinding,
    KnowledgeBaseArticleChange,
)
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.constants.error import ErrorConstants
from gravitino.dto.authorization.owner_dto import OwnerDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.exceptions.base import (
    ArticleRevisionConflictException,
    ForbiddenException,
    KnowledgeBaseArticleAlreadyExistsException,
    MetalakeNotInUseException,
    NoSuchDomainException,
    NoSuchKnowledgeBaseArticleException,
    NoSuchMetadataObjectException,
    NoSuchMetalakeException,
    NoSuchTermException,
    UnapprovedTermException,
)
from gravitino.utils.http_client import HTTPClient


class TestKnowledgeBaseArticleClient(unittest.TestCase):
    ROOT = "/api/metalakes/lake%20%26%20ice/kb-articles"
    ITEM = ROOT + "/Caf%C3%A9"
    NAME = "Cafe\u0301"
    DOMAIN = "税/fees + 100% & sales.ops%2F"

    def setUp(self):
        self.lake = GravitinoMetalake(
            MetalakeDTO("lake & ice", None, {}, None),
            HTTPClient("http://localhost:8090"),
        )
        self.articles = self.lake.article_operations()
        opener = patch("gravitino.utils.http_client.build_opener")
        self.open = opener.start().return_value.open
        self.addCleanup(opener.stop)

    def _respond(self, **data):
        response = MagicMock()
        response.getcode.return_value = 200
        response.read.return_value = json.dumps({"code": 0, **data}).encode()
        self.open.return_value = response

    def _request(self, method, path, query=None):
        request = self.open.call_args.args[0]
        uri = urlsplit(request.full_url)
        self.assertEqual(method, request.get_method())
        self.assertEqual(path, uri.path)
        self.assertEqual(query or {}, parse_qs(uri.query, keep_blank_values=True))
        return json.loads(request.data) if request.data else None

    def test_list_filters_preserve_literal_namespace_components(self):
        namespace = ("first.cat", "库/%2F", "orders + 100%")
        column = ArticleObjectIdentifier(
            MetadataObject.Type.COLUMN, namespace, "sum.amount"
        )
        term = {"name": "Net revenue", "status": "DEPRECATED"}
        owner = {"name": "sales:team", "type": "group"}
        summary = {
            "name": "guide",
            "title": "Guide",
            "status": "PUBLISHED",
            "domains": [self.DOMAIN],
            "owner": owner,
            "terms": [term],
            "termsHasMore": True,
            "audit_info": {"creator": "writer", "createTime": "2026-01-01T00:00:00Z"},
        }
        self._respond(articles=[summary], hasMore=True)
        page = self.articles.list_articles(
            ArticleListFilter(
                query="收入 + 100% & fees",
                domain=self.DOMAIN,
                status=ArticleStatus.PUBLISHED,
                owner=OwnerDTO("sales:team", Owner.Type.GROUP),
                term=term["name"],
                object=column,
                include_deprecated=True,
                limit=2,
                offset=5,
            )
        )
        self._request(
            "GET",
            self.ROOT,
            {
                "q": ["收入 + 100% & fees"],
                "domain": [self.DOMAIN],
                "status": ["PUBLISHED"],
                "owner": ["group:sales:team"],
                "term": [term["name"]],
                "objectType": ["COLUMN"],
                "namespace": list(namespace),
                "name": ["sum.amount"],
                "includeDeprecated": ["true"],
                "limit": ["2"],
                "offset": ["5"],
            },
        )
        self.assertTrue(page.has_more)
        (row,) = page.articles
        self.assertEqual("guide", row.name)
        self.assertEqual(Owner.Type.GROUP, row.owner.type())
        self.assertEqual("writer", row.audit_info.creator())
        self.assertEqual(ArticleTermBinding.Status.DEPRECATED, row.terms[0].status)
        self.assertTrue(row.terms_has_more)

    def test_reverse_term_and_catalog_lookup(self):
        self._respond(articles=[], hasMore=False)
        self.articles.list_articles_for_term("term.税+", include_deprecated=True)
        self._request(
            "GET",
            self.ROOT,
            {
                "term": ["term.税+"],
                "includeDeprecated": ["true"],
                "limit": ["20"],
                "offset": ["0"],
            },
        )
        self.articles.list_articles_for_object(
            ArticleObjectIdentifier(MetadataObject.Type.CATALOG, (), "ice")
        )
        self._request(
            "GET",
            self.ROOT,
            {
                "name": ["ice"],
                "objectType": ["CATALOG"],
                "limit": ["20"],
                "offset": ["0"],
            },
        )

    def test_read_bindings_and_detach_domain(self):
        self._respond(terms=[{"name": "Revenue", "status": "APPROVED"}], hasMore=True)
        page = self.articles.list_term_bindings(self.NAME, limit=1, offset=3)
        self._request("GET", self.ITEM + "/terms", {"limit": ["1"], "offset": ["3"]})
        self.assertEqual("Revenue", page.terms[0].name)
        self.assertTrue(page.has_more)
        self._respond()
        self.articles.detach_domain(self.NAME, self.DOMAIN)
        self._request(
            "DELETE",
            self.ITEM
            + "/domains/%E7%A8%8E%2Ffees%20%2B%20100%25%20%26%20sales.ops%252F",
        )

    def test_load_binding_page_and_edit_with_or_without_token(self):
        self._respond(
            article={
                "name": "Café",
                "title": "Original",
                "status": "PUBLISHED",
                "domains": [],
                "terms": [],
                "termsHasMore": False,
                "objects": [],
                "objectsHasMore": True,
                "synonyms": [],
                "properties": {},
                "owner": None,
                "audit_info": {
                    "creator": "writer",
                    "createTime": "2026-01-01T00:00:00Z",
                },
                "revisionToken": "opaque",
            }
        )
        article = self.articles.load_article(self.NAME, limit=4, offset=2)
        self._request("GET", self.ITEM, {"limit": ["4"], "offset": ["2"]})
        self.assertTrue(article.objects_has_more)
        self.assertFalse(article.terms_has_more)
        for token in (None, article.revision_token):
            with self.subTest(token=token):
                self.articles.alter_article(
                    self.NAME,
                    KnowledgeBaseArticleChange.update_title("New"),
                    KnowledgeBaseArticleChange.update_comment(None),
                    expected_revision=token,
                )
                expected = {
                    "updates": [
                        {"@type": "updateTitle", "newTitle": "New"},
                        {"@type": "updateComment", "newComment": None},
                    ]
                }
                if token is not None:
                    expected["expectedRevision"] = token
                self.assertEqual(expected, self._request("PUT", self.ITEM))

    def test_article_owner_path(self):
        self._respond(owner={"name": "sales", "type": "group"})
        owner = self.lake.get_owner(
            MetadataObjects.of(self.NAME, MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE)
        )
        self._request(
            "GET",
            "/api/metalakes/lake%20%26%20ice/owners/knowledge_base_article/Caf%C3%A9",
        )
        self.assertEqual(Owner.Type.GROUP, owner.type())

    def test_typed_error_responses(self):
        create = ArticleCreate(
            "Guide", ArticleBody(ArticleBody.Format.MARKDOWN, "guide")
        )
        cases = (
            (
                404,
                ErrorConstants.NOT_FOUND_CODE,
                NoSuchKnowledgeBaseArticleException,
                lambda: self.articles.get_article_content(self.NAME),
            ),
            (
                404,
                ErrorConstants.NOT_FOUND_CODE,
                NoSuchDomainException,
                lambda: self.articles.detach_domain(self.NAME, "Finance"),
            ),
            (
                404,
                ErrorConstants.NOT_FOUND_CODE,
                NoSuchTermException,
                lambda: self.articles.list_articles_for_term("Revenue"),
            ),
            (
                404,
                ErrorConstants.NOT_FOUND_CODE,
                NoSuchMetadataObjectException,
                self.articles.list_articles,
            ),
            (
                404,
                ErrorConstants.NOT_FOUND_CODE,
                NoSuchMetalakeException,
                self.articles.list_articles,
            ),
            (
                409,
                ErrorConstants.ALREADY_EXISTS_CODE,
                KnowledgeBaseArticleAlreadyExistsException,
                lambda: self.articles.create_article(create),
            ),
            (
                409,
                ErrorConstants.UNSUPPORTED_OPERATION_CODE,
                UnapprovedTermException,
                lambda: self.articles.create_article(create),
            ),
            (
                409,
                ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE,
                ArticleRevisionConflictException,
                lambda: self.articles.alter_article(
                    self.NAME,
                    KnowledgeBaseArticleChange.update_title("New"),
                    expected_revision="stale",
                ),
            ),
            (
                409,
                ErrorConstants.NOT_IN_USE_CODE,
                MetalakeNotInUseException,
                self.articles.list_articles,
            ),
            (
                403,
                ErrorConstants.FORBIDDEN_CODE,
                ForbiddenException,
                lambda: self.articles.delete_article(self.NAME),
            ),
        )
        for status, code, exception, operation in cases:
            with self.subTest(exception=exception):
                body = {
                    "code": code,
                    "type": exception.__name__,
                    "message": "server detail",
                }
                self.open.side_effect = HTTPError(
                    "url", status, "error", {}, BytesIO(json.dumps(body).encode())
                )
                with self.assertRaisesRegex(exception, "server detail"):
                    operation()
