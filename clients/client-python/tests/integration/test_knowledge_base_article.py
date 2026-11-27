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

import uuid

from gravitino import Catalog, GravitinoAdminClient, NameIdentifier
from gravitino.api.kb import (
    ArticleBody,
    ArticleCreate,
    ArticleListFilter,
    ArticleObjectIdentifier,
    ArticleStatus,
    KnowledgeBaseArticleChange,
)
from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import (
    ArticleRevisionConflictException,
    NoSuchKnowledgeBaseArticleException,
)
from tests.integration.integration_test_env import IntegrationTestEnv


class TestKnowledgeBaseArticle(IntegrationTestEnv):
    def setUp(self):
        admin = GravitinoAdminClient("http://localhost:8090")
        self.addCleanup(admin.close)
        name = "python_kb_" + uuid.uuid4().hex
        self.lake = self.create_metalake_client(admin, name)
        self.addCleanup(admin.drop_metalake, name, True)
        self.addCleanup(self.lake.close)
        self.articles = self.lake.article_operations()

    def test_edit_and_discover_bound_articles(self):
        catalog = self.lake.create_catalog(
            "models", Catalog.Type.MODEL, "model", "", {}
        )
        catalog.as_schemas().create_schema("sales", "", {})
        catalog.as_model_catalog().register_model(
            NameIdentifier.of("sales", "forecast"), "", {}
        )
        target = ArticleObjectIdentifier(
            MetadataObject.Type.MODEL, ("models", "sales"), "forecast"
        )
        original = ArticleBody(ArticleBody.Format.MARKDOWN, "# Original")
        first = self.articles.create_article(
            ArticleCreate("Alpha", original, name="收入", objects=(target,))
        )
        self.assertEqual(ArticleStatus.PUBLISHED, first.status)
        content = self.articles.get_article_content(first.name)
        self.assertEqual(original, content.body)
        self.assertEqual(
            self.articles.load_article(first.name).revision_token,
            content.revision_token,
        )
        edited = ArticleBody(ArticleBody.Format.MARKDOWN, "# Shared 100%")
        updated = self.articles.alter_article(
            first.name,
            KnowledgeBaseArticleChange.update_body(edited),
            expected_revision=content.revision_token,
        )
        self.assertNotEqual(content.revision_token, updated.revision_token)
        with self.assertRaises(ArticleRevisionConflictException):
            self.articles.alter_article(
                first.name,
                KnowledgeBaseArticleChange.update_body(original),
                expected_revision=content.revision_token,
            )
        current = self.articles.get_article_content(first.name)
        self.assertEqual(edited, current.body)
        self.assertEqual(updated.revision_token, current.revision_token)

        second = self.articles.create_article(
            ArticleCreate("Beta", edited, objects=(target,))
        )
        self.assertEqual(ArticleStatus.PUBLISHED, second.status)
        self.assertEqual(
            (target,), self.articles.list_asset_bindings(first.name).objects
        )
        for offset, name, has_more in ((0, first.name, True), (1, second.name, False)):
            with self.subTest(offset=offset):
                for page in (
                    self.articles.list_articles(
                        ArticleListFilter(query="SHARED 100%", limit=1, offset=offset)
                    ),
                    self.articles.list_articles_for_object(
                        target, limit=1, offset=offset
                    ),
                ):
                    self.assertEqual(
                        [name], [article.name for article in page.articles]
                    )
                    self.assertEqual(has_more, page.has_more)

        self.assertTrue(self.articles.delete_article(first.name))
        self.assertFalse(self.articles.delete_article(first.name))
        with self.assertRaises(NoSuchKnowledgeBaseArticleException):
            self.articles.load_article(first.name)
