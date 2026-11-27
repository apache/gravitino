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

"""Ordered mutations supported by Knowledge Base Articles."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Optional, Sequence

from gravitino.api.kb.article import (
    ArticleBody,
    ArticleObjectIdentifier,
    ArticleStatus,
    _names,
    _objects,
    _synonyms,
    _text,
    _title,
)
from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class KnowledgeBaseArticleChange:
    """One Article mutation. Apply multiple changes in order in a single alter call."""

    _fields: dict = field(repr=False)

    def to_dict(self) -> dict:
        """Return the REST operation, including explicitly cleared optional fields."""
        return deepcopy(self._fields)

    @staticmethod
    def update_title(title: str) -> KnowledgeBaseArticleChange:
        """Replace the display title."""
        return KnowledgeBaseArticleChange(
            {"@type": "updateTitle", "newTitle": _title(title)}
        )

    @staticmethod
    def update_body(body: ArticleBody) -> KnowledgeBaseArticleChange:
        """Replace the stored text and markup format."""
        Precondition.check_argument(
            isinstance(body, ArticleBody), "Article body is required"
        )
        return KnowledgeBaseArticleChange(
            {"@type": "updateBody", "newBody": body.to_dict()}
        )

    @staticmethod
    def update_domains(domains: Sequence[str]) -> KnowledgeBaseArticleChange:
        """Replace all Domain memberships."""
        return KnowledgeBaseArticleChange(
            {"@type": "updateDomains", "newDomains": list(_names(domains, "Domains"))}
        )

    @staticmethod
    def update_status(status: ArticleStatus) -> KnowledgeBaseArticleChange:
        """Replace the publication state."""
        Precondition.check_argument(
            isinstance(status, ArticleStatus), "Invalid Article status"
        )
        return KnowledgeBaseArticleChange(
            {"@type": "updateStatus", "newStatus": status.value}
        )

    @staticmethod
    def update_comment(comment: Optional[str]) -> KnowledgeBaseArticleChange:
        """Replace the short comment or clear it with None."""
        return KnowledgeBaseArticleChange(
            {"@type": "updateComment", "newComment": _text(comment, 256, "Comment")}
        )

    @staticmethod
    def update_synonyms(synonyms: Sequence[str]) -> KnowledgeBaseArticleChange:
        """Replace the list of alternative names."""
        return KnowledgeBaseArticleChange(
            {"@type": "updateSynonyms", "newSynonyms": list(_synonyms(synonyms))}
        )

    @staticmethod
    def update_source_reference(
        source_reference: Optional[str],
    ) -> KnowledgeBaseArticleChange:
        """Replace or clear the source reference."""
        return KnowledgeBaseArticleChange(
            {
                "@type": "updateSourceReference",
                "newSourceReference": _text(source_reference, 1024, "Source reference"),
            }
        )

    @staticmethod
    def set_property(key: str, value: str) -> KnowledgeBaseArticleChange:
        """Add or replace an Article property."""
        Precondition.check_argument(
            isinstance(key, str) and bool(key.strip()), "Property key must not be blank"
        )
        Precondition.check_argument(
            isinstance(value, str), "Property value must be a string"
        )
        return KnowledgeBaseArticleChange(
            {"@type": "setProperty", "property": key, "value": value}
        )

    @staticmethod
    def remove_property(key: str) -> KnowledgeBaseArticleChange:
        """Remove an Article property."""
        Precondition.check_argument(
            isinstance(key, str) and bool(key.strip()), "Property key must not be blank"
        )
        return KnowledgeBaseArticleChange({"@type": "removeProperty", "property": key})

    @staticmethod
    def add_terms(terms: Sequence[str]) -> KnowledgeBaseArticleChange:
        """Bind Glossary Terms in the supplied order."""
        return KnowledgeBaseArticleChange(
            {"@type": "addTerms", "terms": list(_names(terms, "Terms"))}
        )

    @staticmethod
    def remove_terms(terms: Sequence[str]) -> KnowledgeBaseArticleChange:
        """Unbind the named Glossary Terms."""
        return KnowledgeBaseArticleChange(
            {"@type": "removeTerms", "terms": list(_names(terms, "Terms"))}
        )

    @staticmethod
    def add_objects(
        objects: Sequence[ArticleObjectIdentifier],
    ) -> KnowledgeBaseArticleChange:
        """Bind the supplied structured asset identifiers."""
        return KnowledgeBaseArticleChange(
            {
                "@type": "addObjects",
                "objects": [obj.to_dict() for obj in _objects(objects)],
            }
        )

    @staticmethod
    def remove_objects(
        objects: Sequence[ArticleObjectIdentifier],
    ) -> KnowledgeBaseArticleChange:
        """Unbind the supplied structured asset identifiers."""
        return KnowledgeBaseArticleChange(
            {
                "@type": "removeObjects",
                "objects": [obj.to_dict() for obj in _objects(objects)],
            }
        )
