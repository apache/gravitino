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

"""Knowledge Base Article inputs and metadata returned by a metalake."""

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping, Optional, Sequence

from gravitino.api.audit import Audit
from gravitino.api.authorization.owner import Owner
from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.authorization.owner_dto import OwnerDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.article_name import normalize_article_name
from gravitino.utils.precondition import Precondition

DEFAULT_LIMIT = 20
MAX_LIMIT = 1000

# The server applies Java String.trim() to names in bindings and synonyms.
_TRIM_CHARS = "".join(map(chr, range(0x21)))

_OBJECT_DEPTH = {
    MetadataObject.Type.CATALOG: 0,
    MetadataObject.Type.SCHEMA: 1,
    MetadataObject.Type.TABLE: 2,
    MetadataObject.Type.VIEW: 2,
    MetadataObject.Type.FILESET: 2,
    MetadataObject.Type.TOPIC: 2,
    MetadataObject.Type.MODEL: 2,
    MetadataObject.Type.SEMANTIC_MODEL: 2,
    MetadataObject.Type.COLUMN: 3,
}


def validate_pagination(limit: int, offset: int) -> None:
    """Check page bounds for an Article or binding list."""
    Precondition.check_argument(
        isinstance(limit, int)
        and not isinstance(limit, bool)
        and 1 <= limit <= MAX_LIMIT,
        f"limit must be between 1 and {MAX_LIMIT}",
    )
    Precondition.check_argument(
        isinstance(offset, int)
        and not isinstance(offset, bool)
        and 0 <= offset <= 2147483647,
        "offset must be a nonnegative 32-bit integer",
    )


def _text(value: Optional[str], maximum: int, field_name: str) -> Optional[str]:
    Precondition.check_argument(
        value is None
        or (isinstance(value, str) and len(value) <= maximum and "\0" not in value),
        f"{field_name} must contain at most {maximum} characters and no U+0000",
    )
    return value


def _title(value: str) -> str:
    Precondition.check_argument(
        isinstance(value, str) and bool(value.strip()), "Title must not be blank"
    )
    return _text(value.strip(), 512, "Title")


def _names(values: Sequence[str], field_name: str) -> tuple[str, ...]:
    Precondition.check_argument(
        isinstance(values, Sequence)
        and not isinstance(values, (str, bytes))
        and all(
            isinstance(value, str) and value.strip(_TRIM_CHARS) for value in values
        ),
        f"{field_name} must be a sequence of nonblank names",
    )
    return tuple(values)


def _synonyms(values: Sequence[str]) -> tuple[str, ...]:
    unique = {}
    for value in _names(values, "Synonyms"):
        trimmed = value.strip(_TRIM_CHARS)
        Precondition.check_argument(
            len(trimmed) <= 128, "Synonym exceeds 128 characters"
        )
        unique.setdefault(_case_insensitive_key(trimmed), trimmed)
        Precondition.check_argument(len(unique) <= 20, "Too many synonyms")
    return tuple(unique.values())


def _case_insensitive_key(value: str) -> str:
    # Java compares one code point at a time, so ß stays distinct from ss.
    folded = []
    for char in value:
        upper = char.upper()
        single = upper if len(upper) == 1 else char
        folded.append(single.lower()[0])
    return "".join(folded)


class ArticleStatus(Enum):
    """Publication state of a Knowledge Base Article."""

    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"
    DEPRECATED = "DEPRECATED"


@dataclass(frozen=True)
class ArticleBody:
    """Stored markup and its format, returned separately from metadata."""

    class Format(Enum):
        """Markup formats accepted by Gravitino."""

        MARKDOWN = "MARKDOWN"
        HTML = "HTML"

    format: Format
    content: str = field(repr=False)

    def __post_init__(self):
        Precondition.check_argument(
            isinstance(self.format, self.Format), "Invalid Article body format"
        )
        Precondition.check_argument(
            isinstance(self.content, str)
            and bool(self.content)
            and "\0" not in self.content,
            "Article body must contain nonempty text without U+0000",
        )
        try:
            self.content.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise IllegalArgumentException(
                "Article body must be valid UTF-8 text"
            ) from exc

    def to_dict(self) -> dict:
        """Return the body as a JSON object."""
        return {"format": self.format.value, "content": self.content}


@dataclass(frozen=True)
class ArticleObjectIdentifier:
    """An asset identifier whose namespace components may contain literal dots."""

    type: MetadataObject.Type
    namespace: Sequence[str]
    name: str

    def __post_init__(self):
        namespace = _names(self.namespace, "Namespace")
        Precondition.check_argument(
            self.type in _OBJECT_DEPTH and len(namespace) == _OBJECT_DEPTH[self.type],
            "Unsupported asset type or incorrect namespace depth",
        )
        Precondition.check_argument(
            isinstance(self.name, str) and bool(self.name.strip(_TRIM_CHARS)),
            "Asset name must not be blank",
        )
        object.__setattr__(self, "namespace", namespace)

    def to_dict(self) -> dict:
        """Return the structured REST identifier."""
        return {
            "type": self.type.name,
            "namespace": list(self.namespace),
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, value: dict) -> "ArticleObjectIdentifier":
        """Read a structured identifier from an Article response."""
        return cls(
            MetadataObject.Type(value["type"].lower()),
            value["namespace"],
            value["name"],
        )


def _objects(
    values: Sequence[ArticleObjectIdentifier],
) -> tuple[ArticleObjectIdentifier, ...]:
    Precondition.check_argument(
        isinstance(values, Sequence)
        and not isinstance(values, (str, bytes))
        and all(isinstance(value, ArticleObjectIdentifier) for value in values),
        "Objects must be a sequence of ArticleObjectIdentifier values",
    )
    return tuple(values)


@dataclass(frozen=True)
class ArticleCreate:  # pylint: disable=too-many-instance-attributes
    """Initial Article fields and ordered bindings. Omit the name to derive it from the title."""

    title: str
    body: ArticleBody
    name: Optional[str] = None
    domains: Sequence[str] = ()
    status: Optional[ArticleStatus] = None
    comment: Optional[str] = None
    synonyms: Sequence[str] = ()
    source_reference: Optional[str] = None
    properties: Mapping[str, str] = field(default_factory=dict)
    objects: Sequence[ArticleObjectIdentifier] = ()
    terms: Sequence[str] = ()

    def __post_init__(self):
        if self.name is not None:
            object.__setattr__(self, "name", normalize_article_name(self.name))
        object.__setattr__(self, "title", _title(self.title))
        Precondition.check_argument(
            isinstance(self.body, ArticleBody), "Article body is required"
        )
        Precondition.check_argument(
            self.status is None or isinstance(self.status, ArticleStatus),
            "Invalid Article status",
        )
        _text(self.comment, 256, "Comment")
        _text(self.source_reference, 1024, "Source reference")
        object.__setattr__(self, "synonyms", _synonyms(self.synonyms))
        object.__setattr__(self, "domains", _names(self.domains, "Domains"))
        object.__setattr__(self, "terms", _names(self.terms, "Terms"))
        object.__setattr__(self, "objects", _objects(self.objects))
        Precondition.check_argument(
            isinstance(self.properties, Mapping)
            and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in self.properties.items()
            ),
            "Article properties must be a string-to-string mapping",
        )
        object.__setattr__(self, "properties", MappingProxyType(dict(self.properties)))

    def to_dict(self) -> dict:
        """Return a JSON create request, omitting unset optional fields."""
        fields = {
            "title": self.title,
            "body": self.body.to_dict(),
            "name": self.name,
            "status": self.status.value if self.status is not None else None,
            "comment": self.comment,
            "sourceReference": self.source_reference,
            "synonyms": list(self.synonyms),
            "domains": list(self.domains),
            "objects": [obj.to_dict() for obj in self.objects],
            "terms": list(self.terms),
            "properties": dict(self.properties),
        }
        return {key: value for key, value in fields.items() if value is not None}


@dataclass(frozen=True)
class ArticleListFilter:  # pylint: disable=too-many-instance-attributes
    """Article search criteria and pagination. Supplied criteria apply together."""

    query: Optional[str] = None
    domain: Optional[str] = None
    status: Optional[ArticleStatus] = None
    owner: Optional[Owner] = None
    term: Optional[str] = None
    object: Optional[ArticleObjectIdentifier] = None
    include_deprecated: bool = False
    limit: int = DEFAULT_LIMIT
    offset: int = 0

    def __post_init__(self):
        validate_pagination(self.limit, self.offset)
        for value in (self.query, self.domain):
            Precondition.check_argument(
                value is None or isinstance(value, str),
                "Query and domain must be strings",
            )
        Precondition.check_argument(
            self.status is None or isinstance(self.status, ArticleStatus),
            "Invalid Article status",
        )
        Precondition.check_argument(
            self.owner is None or isinstance(self.owner, Owner), "Invalid owner"
        )
        if self.term is not None:
            _names((self.term,), "Term")
        Precondition.check_argument(
            self.object is None or isinstance(self.object, ArticleObjectIdentifier),
            "Invalid asset identifier",
        )
        Precondition.check_argument(
            isinstance(self.include_deprecated, bool),
            "include_deprecated must be a boolean",
        )


@dataclass(frozen=True)
class ArticleTermBinding:
    """A visible bound Glossary Term and its current lifecycle state."""

    class Status(Enum):
        """Glossary Term lifecycle state."""

        DRAFT = "DRAFT"
        APPROVED = "APPROVED"
        DEPRECATED = "DEPRECATED"

    name: str
    status: Status

    @classmethod
    def from_dict(cls, value: dict) -> "ArticleTermBinding":
        """Read a Term projection from an Article response."""
        return cls(value["name"], cls.Status(value["status"]))


def _summary_fields(value: dict) -> dict:
    owner = value.get("owner")
    return {
        "name": value["name"],
        "title": value["title"],
        "domains": tuple(value["domains"]),
        "status": ArticleStatus(value["status"]),
        "owner": None if owner is None else OwnerDTO.from_dict(owner),
        "audit_info": AuditDTO.from_dict(value["audit_info"]),
        "terms": tuple(ArticleTermBinding.from_dict(term) for term in value["terms"]),
        "terms_has_more": value["termsHasMore"],
    }


@dataclass(frozen=True)
class ArticleSummary:  # pylint: disable=too-many-instance-attributes
    """Fields projected by Article search, including a page of visible Terms."""

    name: str
    title: str
    domains: tuple[str, ...]
    status: ArticleStatus
    owner: Optional[Owner]
    audit_info: Audit
    terms: tuple[ArticleTermBinding, ...]
    terms_has_more: bool

    @classmethod
    def from_dict(cls, value: dict) -> "ArticleSummary":
        """Read an Article search result."""
        return cls(**_summary_fields(value))


@dataclass(frozen=True)
class KnowledgeBaseArticle(ArticleSummary):
    """Article metadata with the revision token and a page of visible asset and Term bindings."""

    comment: Optional[str]
    synonyms: tuple[str, ...]
    source_reference: Optional[str]
    properties: Mapping[str, str]
    objects: tuple[ArticleObjectIdentifier, ...]
    objects_has_more: bool
    revision_token: str

    @classmethod
    def from_dict(cls, value: dict) -> "KnowledgeBaseArticle":
        """Read the Article metadata object in a load, create or alter response."""
        return cls(
            **_summary_fields(value),
            comment=value.get("comment"),
            synonyms=tuple(value["synonyms"]),
            source_reference=value.get("sourceReference"),
            properties=MappingProxyType(dict(value["properties"])),
            objects=tuple(
                ArticleObjectIdentifier.from_dict(obj) for obj in value["objects"]
            ),
            objects_has_more=value["objectsHasMore"],
            revision_token=value["revisionToken"],
        )


@dataclass(frozen=True)
class ArticleContent:
    """Article body and editing token from the same revision."""

    body: ArticleBody
    revision_token: str

    @classmethod
    def from_dict(cls, value: dict) -> "ArticleContent":
        """Read the content response from the server."""
        body = value["body"]
        return cls(
            ArticleBody(ArticleBody.Format(body["format"]), body["content"]),
            value["revisionToken"],
        )


@dataclass(frozen=True)
class ArticlePage:
    """A page of visible Article search results."""

    articles: tuple[ArticleSummary, ...]
    has_more: bool

    @classmethod
    def from_dict(cls, value: dict) -> "ArticlePage":
        """Read the list response from the server."""
        return cls(
            tuple(ArticleSummary.from_dict(article) for article in value["articles"]),
            value["hasMore"],
        )


@dataclass(frozen=True)
class AssetBindingPage:
    """A page of visible asset identifiers."""

    objects: tuple[ArticleObjectIdentifier, ...]
    has_more: bool

    @classmethod
    def from_dict(cls, value: dict) -> "AssetBindingPage":
        """Read the asset binding response from the server."""
        return cls(
            tuple(ArticleObjectIdentifier.from_dict(obj) for obj in value["objects"]),
            value["hasMore"],
        )


@dataclass(frozen=True)
class TermBindingPage:
    """A page of visible Glossary Terms."""

    terms: tuple[ArticleTermBinding, ...]
    has_more: bool

    @classmethod
    def from_dict(cls, value: dict) -> "TermBindingPage":
        """Read the Term binding response from the server."""
        return cls(
            tuple(ArticleTermBinding.from_dict(term) for term in value["terms"]),
            value["hasMore"],
        )
