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

import unittest

from gravitino.api.authorization.owner import Owner
from gravitino.api.kb import (
    ArticleBody,
    ArticleContent,
    ArticleCreate,
    ArticleListFilter,
    ArticleObjectIdentifier,
    ArticlePage,
    ArticleStatus,
    ArticleTermBinding,
    AssetBindingPage,
    KnowledgeBaseArticle,
    KnowledgeBaseArticleChange as Change,
    TermBindingPage,
)
from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import IllegalArgumentException


class TestArticleModels(unittest.TestCase):
    def setUp(self):
        self.body = ArticleBody(ArticleBody.Format.MARKDOWN, "# 📚")
        self.obj = ArticleObjectIdentifier(
            MetadataObject.Type.COLUMN, ["c.at", "s/ch", "table"], "a.b"
        )
        self.obj_json = {
            "type": "COLUMN",
            "namespace": ["c.at", "s/ch", "table"],
            "name": "a.b",
        }
        self.second = ArticleObjectIdentifier(MetadataObject.Type.CATALOG, [], "c")
        self.second_json = {"type": "CATALOG", "namespace": [], "name": "c"}

    def test_create_request(self):
        create = ArticleCreate(
            "  Sales  ",
            self.body,
            name="Cafe\u0301",
            domains=["Finance", "Sales"],
            synonyms=[" aliases ", "ALIASES", "facts"],
            objects=[self.obj, self.second],
            terms=["B", "A"],
            properties={"p": ""},
            source_reference="guide",
            comment="",
        )
        self.assertEqual(
            create.to_dict(),
            {
                "title": "Sales",
                "name": "Café",
                "body": {"format": "MARKDOWN", "content": "# 📚"},
                "domains": ["Finance", "Sales"],
                "synonyms": ["aliases", "facts"],
                "sourceReference": "guide",
                "terms": ["B", "A"],
                "objects": [self.obj_json, self.second_json],
                "comment": "",
                "properties": {"p": ""},
            },
        )
        generated = ArticleCreate(
            "Sales", self.body, status=ArticleStatus.PUBLISHED
        ).to_dict()
        self.assertNotIn("name", generated)
        self.assertNotIn("comment", generated)
        self.assertEqual(generated["status"], "PUBLISHED")

    def test_all_changes(self):
        cases = [
            (
                Change.update_title(" title "),
                {"@type": "updateTitle", "newTitle": "title"},
            ),
            (
                Change.update_body(self.body),
                {
                    "@type": "updateBody",
                    "newBody": {"format": "MARKDOWN", "content": "# 📚"},
                },
            ),
            (Change.update_domains([]), {"@type": "updateDomains", "newDomains": []}),
            (
                Change.update_status(ArticleStatus.DEPRECATED),
                {"@type": "updateStatus", "newStatus": "DEPRECATED"},
            ),
            (
                Change.update_comment(None),
                {"@type": "updateComment", "newComment": None},
            ),
            (
                Change.update_synonyms([" S ", "s"]),
                {"@type": "updateSynonyms", "newSynonyms": ["S"]},
            ),
            (
                Change.update_source_reference(None),
                {"@type": "updateSourceReference", "newSourceReference": None},
            ),
            (
                Change.set_property("key", "v"),
                {"@type": "setProperty", "property": "key", "value": "v"},
            ),
            (
                Change.remove_property("key"),
                {"@type": "removeProperty", "property": "key"},
            ),
            (Change.add_terms(["B", "A"]), {"@type": "addTerms", "terms": ["B", "A"]}),
            (Change.remove_terms(["T"]), {"@type": "removeTerms", "terms": ["T"]}),
            (
                Change.add_objects([self.second, self.obj]),
                {"@type": "addObjects", "objects": [self.second_json, self.obj_json]},
            ),
            (
                Change.remove_objects([self.obj]),
                {"@type": "removeObjects", "objects": [self.obj_json]},
            ),
        ]
        for change, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(change.to_dict(), expected)

    def test_text_boundaries(self):
        astral_letter = "\U00010400"
        create = ArticleCreate(
            astral_letter * 512,
            self.body,
            name=astral_letter * 128,
            comment="📝" * 256,
            synonyms=["📝" * 128],
            source_reference="📝" * 1024,
        )
        self.assertEqual(len(create.title), 512)
        self.assertEqual(
            Change.update_synonyms(
                ["i", "İ", "ı", "Σ", "ς", "Straße", "STRASSE"]
            ).to_dict()["newSynonyms"],
            ["i", "Σ", "Straße", "STRASSE"],
        )
        for fields in (
            {"title": astral_letter * 513},
            {"name": astral_letter * 129},
            {"name": "-_\u0301"},
            {"name": "article/1"},
            {"comment": "📝" * 257},
            {"synonyms": ["📝" * 129]},
            {"synonyms": [str(i) for i in range(21)]},
            {"source_reference": "📝" * 1025},
            {"title": "title\0"},
            {"comment": "\0"},
            {"source_reference": "ref\0"},
            {"domains": [" "]},
            {"terms": "abc"},
            {"objects": [None]},
            {"properties": {"a": None}},
        ):
            with self.subTest(fields=fields), self.assertRaises(
                IllegalArgumentException
            ):
                ArticleCreate(**{"title": "title", "body": self.body, **fields})
        for content in ("", "text\0", "\ud800"):
            with self.subTest(content=content), self.assertRaises(
                IllegalArgumentException
            ):
                ArticleBody(ArticleBody.Format.HTML, content)
        # The limit on stored UTF-8 bytes is a server setting.
        self.assertEqual(
            len(ArticleBody(ArticleBody.Format.HTML, "x" * 1048577).content), 1048577
        )

    def test_filters_and_binding_identifiers(self):
        self.assertEqual(ArticleListFilter().limit, 20)
        self.assertEqual(
            ArticleListFilter(limit=1000, offset=5, object=self.obj).object, self.obj
        )
        for fields in (
            {"limit": 0},
            {"limit": 1001},
            {"limit": True},
            {"offset": -1},
            {"offset": 0.5},
            {"term": " "},
            {"include_deprecated": 1},
        ):
            with self.subTest(fields=fields), self.assertRaises(
                IllegalArgumentException
            ):
                ArticleListFilter(**fields)
        for obj_type, namespace in (
            (MetadataObject.Type.CATALOG, ["extra"]),
            (MetadataObject.Type.TABLE, ["cat.schema"]),
            (MetadataObject.Type.TABLE, ["cat", " "]),
            (MetadataObject.Type.COLUMN, "abc"),
            (MetadataObject.Type.ROLE, []),
        ):
            with self.subTest(obj_type=obj_type), self.assertRaises(
                IllegalArgumentException
            ):
                ArticleObjectIdentifier(obj_type, namespace, "name")

    def test_response_projections(self):
        terms = [
            {"name": "older", "status": "DEPRECATED"},
            {"name": "approved", "status": "APPROVED"},
        ]
        summary = {
            "name": "a",
            "title": "A",
            "domains": ["d"],
            "status": "PUBLISHED",
            "owner": None,
            "terms": terms,
            "termsHasMore": False,
            "audit_info": {"creator": "user", "createTime": "2025-02-01T00:00:00Z"},
        }
        detail = KnowledgeBaseArticle.from_dict(
            {
                **summary,
                "owner": {"name": "team", "type": "GROUP"},
                "termsHasMore": True,
                "objects": [self.obj_json, self.second_json],
                "objectsHasMore": False,
                "synonyms": [],
                "properties": {"source": "x"},
                "revisionToken": "opaque",
            }
        )
        self.assertEqual(detail.owner.name(), "team")
        self.assertEqual(detail.owner.type(), Owner.Type.GROUP)
        self.assertEqual(detail.audit_info.creator(), "user")
        self.assertEqual(detail.audit_info.create_time().year, 2025)
        self.assertEqual(detail.revision_token, "opaque")
        self.assertEqual(detail.objects, (self.obj, self.second))
        self.assertTrue(detail.terms_has_more)
        self.assertFalse(detail.objects_has_more)
        self.assertIsNone(detail.source_reference)
        self.assertIsNone(detail.comment)
        page = ArticlePage.from_dict({"articles": [summary], "hasMore": True})
        self.assertTrue(page.has_more)
        self.assertIsNone(page.articles[0].owner)
        self.assertFalse(hasattr(page.articles[0], "revision_token"))
        self.assertFalse(page.articles[0].terms_has_more)
        self.assertEqual(
            page.articles[0].terms,
            (
                ArticleTermBinding("older", ArticleTermBinding.Status.DEPRECATED),
                ArticleTermBinding("approved", ArticleTermBinding.Status.APPROVED),
            ),
        )
        self.assertEqual(
            TermBindingPage.from_dict({"terms": terms, "hasMore": False}),
            TermBindingPage(detail.terms, False),
        )
        self.assertEqual(
            AssetBindingPage.from_dict({"objects": [], "hasMore": False}),
            AssetBindingPage((), False),
        )
        content = ArticleContent.from_dict(
            {"body": {"format": "HTML", "content": "<p>a</p>"}, "revisionToken": "r2"}
        )
        self.assertEqual(
            content,
            ArticleContent(ArticleBody(ArticleBody.Format.HTML, "<p>a</p>"), "r2"),
        )

    def test_body_repr(self):
        for value in (
            self.body,
            ArticleCreate("title", self.body),
            Change.update_body(self.body),
            ArticleContent(self.body, "r1"),
        ):
            self.assertNotIn(self.body.content, repr(value))
