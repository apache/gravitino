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
from dataclasses import FrozenInstanceError
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from gravitino.api.file.fileset import Fileset
from gravitino.api.model.model import Model
from gravitino.api.rel.table import Table
from gravitino.api.rel.view import View
from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.domain import (
    Domain,
    DomainChange,
    DomainMemberFilter,
    DomainRelation,
    SupportsDomains,
)
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.tag.tag_value import TagValue
from gravitino.client.generic_domain import GenericDomain
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.generic_model import GenericModel
from gravitino.client.generic_semantic_model import GenericSemanticModel
from gravitino.client.generic_view import GenericView
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.client.metadata_object_domain_operations import (
    MetadataObjectDomainOperations,
)
from gravitino.client.relational_table import RelationalTable
from gravitino.constants.error import ErrorConstants
from gravitino.dto.domain_dto import DomainDTO
from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.requests.domain_create_request import (
    BackingTagCreate,
    DomainCreateRequest,
)
from gravitino.dto.requests.domain_set_request import (
    DomainBackingTagSetRequest,
    DomainParentSetRequest,
    DomainSetRequest,
)
from gravitino.dto.requests.domain_update_request import DomainUpdateRequest
from gravitino.dto.requests.domain_updates_request import DomainUpdatesRequest
from gravitino.dto.responses.domain_response import (
    DomainListResponse,
    DomainNamesListResponse,
    DomainResponse,
)
from gravitino.exceptions.base import (
    BackingTagAlreadyBoundException,
    DomainAlreadyExistsException,
    ForbiddenException,
    IllegalArgumentException,
    MetalakeNotInUseException,
    NoSuchDomainException,
    NoSuchMetalakeException,
    NoSuchMetadataObjectException,
    NoSuchTagException,
    NonEmptyDomainException,
    NotEmptyException,
    OptimisticLockException,
    RESTException,
    TagAlreadyExistsException,
    UnauthorizedException,
    UnsupportedOperationException,
)
from gravitino.namespace import Namespace
from gravitino.rest.rest_utils import encode_string
from gravitino.utils.http_client import HTTPClient


class TestDomainClient(unittest.TestCase):
    METALAKE = "lake/with. spaces?&+%"
    NAME = "Finance/US. East,?&+%"

    def setUp(self):
        self.client = HTTPClient("http://localhost:8090")
        self.metalake = GravitinoMetalake(
            MetalakeDTO(self.METALAKE, None, {}, None), self.client
        )
        self.opener_patch = patch("gravitino.utils.http_client.build_opener")
        self.opener = self.opener_patch.start().return_value
        self.addCleanup(self.opener_patch.stop)
        self.path = f"/api/metalakes/{encode_string(self.METALAKE)}/domains"

    def _dto(self, **overrides):
        data = {
            "name": self.NAME,
            "comment": "comment",
            "properties": {"a": "b"},
            "status": "draft",
            "parent": None,
            "backingTag": {"name": "department", "value": "finance"},
            "audit": {"creator": "user", "createTime": "2026-10-01T00:00:00Z"},
        }
        data.update(overrides)
        return data

    def _respond(self, body):
        response = MagicMock()
        response.getcode.return_value = 200
        response.read.return_value = json.dumps(body).encode()
        self.opener.open.return_value = response
        self.opener.open.side_effect = None

    def _domain(self, **overrides):
        return GenericDomain(
            self.METALAKE, DomainDTO.from_dict(self._dto(**overrides)), self.client
        )

    def _request(self, method, path, body=None, query=None):
        request = self.opener.open.call_args.args[0]
        self.assertEqual(method, request.get_method())
        self.assertEqual(path, urlsplit(request.full_url).path)
        self.assertEqual(query or {}, parse_qs(urlsplit(request.full_url).query))
        self.assertEqual(body, json.loads(request.data) if request.data else None)
        return request

    def test_snapshot_fields_and_defensive_copies(self):
        dto = DomainDTO.from_dict(self._dto())
        domain = GenericDomain(self.METALAKE, dto, self.client)
        dto._properties["a"] = "changed"  # pylint: disable=protected-access
        self.assertEqual(self.NAME, domain.name())
        self.assertEqual("comment", domain.comment())
        self.assertEqual(Domain.Status.DRAFT, domain.status())
        self.assertIsNone(domain.parent())
        self.assertEqual(TagValue("department", "finance"), domain.backing_tag())
        self.assertEqual("user", domain.audit_info().creator())
        domain.properties()["a"] = "changed"
        self.assertEqual({"a": "b"}, domain.properties())
        self.assertEqual([], domain.relations())
        self.assertIsNone(self._domain(backingTag=None).backing_tag())

    def test_create_top_level_and_subdomain(self):
        for parent in (None, self.NAME):
            for create, tag in (
                (True, TagValue("new/tag")),
                (False, TagValue("dept", "finance")),
            ):
                with self.subTest(parent=parent, create=create):
                    self._respond(
                        {"code": 0, "domain": self._dto(name="child", parent=parent)}
                    )
                    if parent is None:
                        domain = self.metalake.create_domain(
                            "child", None, None, tag, create
                        )
                    else:
                        domain = self._domain().create_subdomain(
                            "child", None, None, tag, create
                        )
                    self.assertEqual("child", domain.name())
                    self.assertEqual(parent, domain.parent())
                    self._request(
                        "POST",
                        self.path,
                        {
                            "name": "child",
                            "comment": None,
                            "properties": None,
                            "parent": parent,
                            "backingTag": {
                                "name": tag.name(),
                                "value": tag.value(),
                                "create": create,
                            },
                        },
                    )

    def test_list_and_get_domains(self):
        self._respond({"code": 0, "names": [self.NAME]})
        self.assertEqual([self.NAME], self.metalake.list_domains())
        self._request("GET", self.path)
        self._respond({"code": 0, "domains": [self._dto()]})
        self.assertEqual(self.NAME, self.metalake.list_domains_info()[0].name())
        self._request("GET", self.path, query={"details": ["true"]})
        self._respond({"code": 0, "domain": self._dto()})
        self.assertEqual(self.NAME, self.metalake.get_domain(self.NAME).name())
        self._request("GET", self.path + "/" + encode_string(self.NAME))

    def test_list_domains_rejects_malformed_names(self):
        for names in ("Tax", [None], ["Tax", 123], {"Tax": True}):
            with self.subTest(names=names):
                self._respond({"code": 0, "names": names})
                with self.assertRaisesRegex(
                    IllegalArgumentException, "names must be an array of strings"
                ):
                    self.metalake.list_domains()

    def test_list_responses_require_explicit_arrays(self):
        for field, operation in (
            ("names", self.metalake.list_domains),
            ("domains", self.metalake.list_domains_info),
            ("members", lambda: self._domain().list_members(DomainMemberFilter())),
        ):
            for payload in ({"code": 0}, {"code": 0, field: None}):
                with self.subTest(field=field, payload=payload):
                    self._respond(payload)
                    with self.assertRaises(IllegalArgumentException):
                        operation()
            with self.subTest(field=field, value=[]):
                self._respond({"code": 0, field: []})
                self.assertEqual([], operation())

    def test_alter_domain_serializes_all_changes(self):
        self._respond({"code": 0, "domain": self._dto(name="renamed")})
        result = self.metalake.alter_domain(
            self.NAME,
            DomainChange.rename("renamed"),
            DomainChange.update_comment(None),
            DomainChange.set_property("key", ""),
            DomainChange.remove_property("old"),
        )
        self.assertEqual("renamed", result.name())
        self._request(
            "PUT",
            self.path + "/" + encode_string(self.NAME),
            {
                "updates": [
                    {"@type": "rename", "newName": "renamed"},
                    {"@type": "updateComment", "newComment": None},
                    {"@type": "setProperty", "property": "key", "value": ""},
                    {"@type": "removeProperty", "property": "old"},
                ]
            },
        )

    def test_empty_alter_domain(self):
        self._respond({"code": 0, "domain": self._dto()})
        self.assertEqual(self._domain(), self.metalake.alter_domain(self.NAME))
        self._request(
            "PUT", self.path + "/" + encode_string(self.NAME), {"updates": []}
        )

    def test_snapshot_equality(self):
        dto = DomainDTO.from_dict(self._dto())
        other_client = GenericDomain(self.METALAKE, dto, MagicMock())
        self.assertEqual({self._domain(), other_client}, {other_client})
        self.assertNotEqual(other_client, GenericDomain("other", dto, self.client))
        for changes in (
            {"name": "new"},
            {"comment": None},
            {"properties": {}},
            {"status": "published"},
            {"parent": "parent"},
            {"backingTag": None},
            {"audit": {"creator": "other"}},
            {"relations": [{"relation": "direct"}]},
        ):
            with self.subTest(changes=changes):
                self.assertNotEqual(other_client, self._domain(**changes))

    def test_invalid_inherited_source(self):
        for source in (None, {"fullName": "cat.schema.table", "type": "table"}):
            domain = self._domain(
                relations=[{"relation": "inherited", "source": source}]
            )
            with self.assertRaises(IllegalArgumentException):
                domain.relations()

    def test_dot_segment_snapshot(self):
        for name in (".", ".."):
            domain = self._domain(name=name)
            self.assertEqual(name, domain.name())
            with self.assertRaises(IllegalArgumentException):
                domain.publish()
        self.opener.open.assert_not_called()

    def test_delete_domain(self):
        for dropped in (True, False):
            self._respond({"code": 0, "dropped": dropped})
            self.assertEqual(dropped, self.metalake.delete_domain(self.NAME))
            self._request("DELETE", self.path + "/" + encode_string(self.NAME))

    def test_mutations_return_new_snapshots_addressed_by_old_name(self):
        old = self._domain()
        operations = (
            (
                old.publish,
                "PATCH",
                "",
                {"status": "published"},
                {"status": "published"},
            ),
            (old.unpublish, "PATCH", "", {"status": "draft"}, {"status": "draft"}),
            (
                lambda: old.attach_to_parent("Parent/with. spaces"),
                "PUT",
                "/parent",
                {"parent": "Parent/with. spaces"},
                {"parent": "Parent/with. spaces"},
            ),
            (old.detach_from_parent, "DELETE", "/parent", None, {"parent": None}),
            (
                lambda: old.update_backing_tag(TagValue("tag/new", "v")),
                "PUT",
                "/backingTag",
                {"backingTag": {"name": "tag/new", "value": "v"}},
                {"backingTag": {"name": "tag/new", "value": "v"}},
            ),
            (
                lambda: old.update_backing_tag(None),
                "PUT",
                "/backingTag",
                {"backingTag": None},
                {"backingTag": None},
            ),
        )
        for operation, method, suffix, body, changes in operations:
            with self.subTest(method=method, body=body):
                self._respond({"code": 0, "domain": self._dto(**changes)})
                result = operation()
                self.assertIsNot(old, result)
                self.assertEqual(Domain.Status.DRAFT, old.status())
                self.assertIsNone(old.parent())
                self.assertEqual(TagValue("department", "finance"), old.backing_tag())
                self.assertEqual(
                    DomainDTO.from_dict(self._dto(**changes)).status(), result.status()
                )
                self.assertEqual(changes.get("parent"), result.parent())
                self.assertEqual(
                    self._domain(**changes).backing_tag(), result.backing_tag()
                )
                self._request(
                    method, self.path + "/" + encode_string(self.NAME) + suffix, body
                )
        # A rename result has a new name; the existing snapshot keeps addressing its own name.
        self._respond(
            {"code": 0, "domain": self._dto(name="replacement", status="published")}
        )
        self.assertEqual("replacement", old.publish().name())
        self._request(
            "PATCH", self.path + "/" + encode_string(self.NAME), {"status": "published"}
        )
        self.assertEqual(self.NAME, old.name())

    def test_list_subdomains(self):
        domain = self._domain()
        self._respond({"code": 0, "names": ["Tax"]})
        self.assertEqual(["Tax"], domain.list_subdomains())
        path = self.path + "/" + encode_string(self.NAME) + "/subdomains"
        self._request("GET", path)
        self._respond({"code": 0, "domains": [self._dto(name="Tax", parent=self.NAME)]})
        child = domain.list_subdomains_info()[0]
        self.assertEqual("Tax", child.name())
        self.assertEqual(self.NAME, child.parent())
        self._request("GET", path, query={"details": ["true"]})

    def test_members_filters_and_relations(self):
        relations = [
            {"relation": "direct"},
            {
                "relation": "inherited",
                "source": {"fullName": "cat.schema", "type": "schema"},
            },
            {
                "relation": "via_subdomain",
                "source": {"fullName": "Trading.US/a b", "type": "domain"},
            },
        ]
        self._respond(
            {
                "code": 0,
                "members": [
                    {
                        "object": {"fullName": "cat.schema.table", "type": "table"},
                        "relations": relations,
                    }
                ],
            }
        )
        scopes = ("Tax,EU", "Trading/US. East&?+%")
        member_filter = DomainMemberFilter(
            types=(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW),
            name_contains="a b/&?+=%,",
            domains=scopes,
        )
        reversed_filter = DomainMemberFilter(
            types=tuple(reversed(member_filter.types)),
            name_contains=member_filter.name_contains,
            domains=tuple(reversed(scopes)),
        )
        self.assertEqual({member_filter}, {reversed_filter})
        member = self._domain().list_members(member_filter)[0]
        self._request(
            "GET",
            self.path + "/" + encode_string(self.NAME) + "/objects",
            query={
                "types": ["TABLE,VIEW"],
                "nameContains": ["a b/&?+=%,"],
                "domains": list(scopes),
            },
        )
        self.assertEqual(MetadataObject.Type.TABLE, member.object().type())
        self.assertEqual("cat.schema.table", member.object().full_name())
        self.assertEqual(
            [
                DomainRelation.Type.DIRECT,
                DomainRelation.Type.INHERITED,
                DomainRelation.Type.VIA_SUBDOMAIN,
            ],
            [r.type() for r in member.relations()],
        )
        self.assertIsNone(member.relations()[0].source())
        self.assertEqual("cat.schema", member.relations()[1].source().full_name())
        self.assertEqual("Trading.US/a b", member.relations()[2].source().name())
        self.assertIsNone(member.relations()[2].source().parent())
        self.assertEqual(
            MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE),
            member.object(),
        )
        self.assertEqual({member}, {self._domain().list_members(member_filter)[0]})
        other = GenericDomain("other", DomainDTO.from_dict(self._dto()), self.client)
        self.assertNotEqual(member, other.list_members(member_filter)[0])
        member.relations().clear()
        self.assertEqual(3, len(member.relations()))
        self._respond({"code": 0, "members": []})
        self.assertEqual([], self._domain().list_members(DomainMemberFilter()))
        self._request("GET", self.path + "/" + encode_string(self.NAME) + "/objects")

    def test_single_domain_filter(self):
        self._respond({"code": 0, "members": []})
        self._domain().list_members(DomainMemberFilter(domains="Tax"))
        self._request(
            "GET",
            self.path + "/" + encode_string(self.NAME) + "/objects",
            query={"domains": ["Tax"]},
        )

    def test_domain_identifier_parent(self):
        for parent in ("Finance", ""):
            with self.subTest(parent=parent), self.assertRaises(
                IllegalArgumentException
            ):
                MetadataObjects.of("Tax", MetadataObject.Type.DOMAIN, parent=parent)
        domain = MetadataObjects.of("Finance.Tax", MetadataObject.Type.DOMAIN)
        self.assertEqual("Finance.Tax", domain.name())
        self.assertIsNone(domain.parent())

    def test_leaf_lookup_all_six_types(self):
        for object_type in (
            MetadataObject.Type.TABLE,
            MetadataObject.Type.VIEW,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.SEMANTIC_MODEL,
        ):
            with self.subTest(object_type=object_type):
                name = "cat.schema.leaf/with spaces?&+%"
                operations = MetadataObjectDomainOperations(
                    self.METALAKE, MetadataObjects.parse(name, object_type), self.client
                )
                path = (
                    f"/api/metalakes/{encode_string(self.METALAKE)}/objects/"
                    f"{object_type.value}/{encode_string(name)}/domains"
                )
                self._respond({"code": 0, "names": [self.NAME]})
                self.assertEqual([self.NAME], operations.list_domains())
                self._request("GET", path)
                self._respond(
                    {
                        "code": 0,
                        "domains": [self._dto(relations=[{"relation": "direct"}])],
                    }
                )
                domain = operations.list_domains_info()[0]
                self.assertEqual(
                    DomainRelation.Type.DIRECT, domain.relations()[0].type()
                )
                self._request("GET", path, query={"details": ["true"]})

    def test_existing_leaf_resources_delegate(self):
        namespace = Namespace.of(self.METALAKE, "cat", "schema")
        dto = MagicMock()
        dto.name.return_value = "leaf/with spaces"
        dto.properties.return_value = {}
        constructors = (
            (GenericFileset, lambda: GenericFileset(dto, self.client, namespace)),
            (GenericView, lambda: GenericView(dto, self.client, namespace)),
            (GenericModel, lambda: GenericModel(dto, self.client, namespace)),
            (
                GenericSemanticModel,
                lambda: GenericSemanticModel(dto, self.client, namespace),
            ),
        )
        for resource_type, constructor in constructors:
            with self.subTest(resource_type=resource_type):
                resource = constructor()
                self.assertIsInstance(resource, SupportsDomains)
                self.assertIs(resource, resource.supports_domains())
                self._respond({"code": 0, "names": [self.NAME]})
                self.assertEqual([self.NAME], resource.list_domains())
                self._respond({"code": 0, "domains": [self._dto()]})
                self.assertEqual(self.NAME, resource.list_domains_info()[0].name())
        with patch(
            "gravitino.client.relational_table.DTOConverters.from_dto",
            return_value=MagicMock(),
        ):
            table = RelationalTable(namespace, dto, self.client)
        self.assertIs(table, table.supports_domains())
        self._respond({"code": 0, "names": [self.NAME]})
        self.assertEqual([self.NAME], table.list_domains())
        self._respond({"code": 0, "domains": [self._dto()]})
        self.assertEqual(self.NAME, table.list_domains_info()[0].name())

    def test_http_error_mapping(self):
        cases = (
            (404, ErrorConstants.NOT_FOUND_CODE, NoSuchDomainException),
            (404, ErrorConstants.NOT_FOUND_CODE, NoSuchMetalakeException),
            (404, ErrorConstants.NOT_FOUND_CODE, NoSuchTagException),
            (404, ErrorConstants.NOT_FOUND_CODE, NoSuchMetadataObjectException),
            (403, ErrorConstants.FORBIDDEN_CODE, ForbiddenException),
            (401, ErrorConstants.UNAUTHORIZED_CODE, UnauthorizedException),
            (400, ErrorConstants.ILLEGAL_ARGUMENTS_CODE, IllegalArgumentException),
            (409, ErrorConstants.ALREADY_EXISTS_CODE, DomainAlreadyExistsException),
            (409, ErrorConstants.ALREADY_EXISTS_CODE, TagAlreadyExistsException),
            (409, ErrorConstants.ALREADY_EXISTS_CODE, BackingTagAlreadyBoundException),
            (409, ErrorConstants.NON_EMPTY_CODE, NonEmptyDomainException),
            (409, ErrorConstants.UNSUPPORTED_OPERATION_CODE, NonEmptyDomainException),
            (
                409,
                ErrorConstants.UNSUPPORTED_OPERATION_CODE,
                UnsupportedOperationException,
            ),
            (409, ErrorConstants.NOT_IN_USE_CODE, MetalakeNotInUseException),
            (
                409,
                ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE,
                OptimisticLockException,
            ),
            (500, ErrorConstants.INTERNAL_ERROR_CODE, RuntimeError),
            (500, ErrorConstants.REST_ERROR_CODE, RESTException),
        )
        for http_status, code, exception in cases:
            with self.subTest(exception=exception):
                error = {
                    "code": code,
                    "type": exception.__name__,
                    "message": "server error",
                    "stack": None,
                }
                self.opener.open.side_effect = HTTPError(
                    "url", http_status, "error", {}, BytesIO(json.dumps(error).encode())
                )
                with self.assertRaisesRegex(exception, "server error"):
                    self.metalake.get_domain(self.NAME)

    def test_nonempty_domain_can_be_caught_as_not_empty(self):
        body = {
            "code": ErrorConstants.NON_EMPTY_CODE,
            "type": "NonEmptyDomainException",
            "message": "Domain still has children",
        }
        self.opener.open.side_effect = HTTPError(
            "url", 409, "Conflict", {}, BytesIO(json.dumps(body).encode())
        )
        with self.assertRaises(NotEmptyException) as caught:
            self.metalake.delete_domain(self.NAME)
        self.assertIsInstance(caught.exception, NonEmptyDomainException)

    def test_server_rejects_invalid_domain_name(self):
        error = {
            "code": ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
            "type": "IllegalArgumentException",
            "message": "reserved domain",
            "stack": None,
        }
        self.opener.open.side_effect = HTTPError(
            "url", 400, "error", {}, BytesIO(json.dumps(error).encode())
        )
        with self.assertRaisesRegex(IllegalArgumentException, "reserved domain"):
            self.metalake.get_domain("SyStEm")
        self._request("GET", self.path + "/SyStEm")

    def test_validation_before_http(self):
        for name in (None, "", " ", ".", ".."):
            for operation in (
                self.metalake.get_domain,
                self.metalake.delete_domain,
                lambda n: self.metalake.get_owner(
                    MetadataObjects.MetadataObjectImpl(n, MetadataObject.Type.DOMAIN)
                ),
                lambda n: self.metalake.alter_domain(n, DomainChange.rename("new")),
            ):
                with self.subTest(name=name, operation=operation):
                    with self.assertRaises(IllegalArgumentException):
                        operation(name)
        for blank in ("", " ", "\t"):
            with self.subTest(blank=blank):
                for request in (
                    DomainCreateRequest(blank, None, {}, None, BackingTagCreate("tag")),
                    DomainCreateRequest(
                        "child", None, {}, blank, BackingTagCreate("tag")
                    ),
                    DomainParentSetRequest(blank),
                    DomainUpdateRequest.RenameDomainRequest(blank),
                ):
                    with self.assertRaises(IllegalArgumentException):
                        request.validate()
        invalid_operations = (
            lambda: self.metalake.create_domain("name", None, None, None, False),
            lambda: self.metalake.create_domain(
                "name", None, None, TagValue("tag", "v"), True
            ),
            lambda: self.metalake.create_domain(
                None, None, None, TagValue("tag"), False
            ),
            lambda: self.metalake.create_domain(
                "name", None, {"key": None}, TagValue("tag"), False
            ),
            lambda: self.metalake.alter_domain("name", None),
            lambda: self.metalake.alter_domain("name", DomainChange.rename(None)),
            lambda: self.metalake.alter_domain(
                "name", DomainChange.set_property(" ", "value")
            ),
            lambda: self.metalake.alter_domain(
                "name", DomainChange.set_property("key", None)
            ),
            lambda: self.metalake.alter_domain(
                "name", DomainChange.remove_property(" ")
            ),
            lambda: self._domain().attach_to_parent(None),
            lambda: self._domain().create_subdomain("child", None, None, None, False),
            lambda: self._domain().list_members(None),
            lambda: MetadataObjectDomainOperations(
                self.METALAKE,
                MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA),
                self.client,
            ),
        )
        for operation in invalid_operations:
            with self.subTest(operation=operation):
                with self.assertRaises(IllegalArgumentException):
                    operation()
        self.opener.open.assert_not_called()

    def test_tag_value_utf16_length(self):
        for value in ("x" * 256, "😀" * 128):
            self.assertEqual(value, TagValue("tag", value).value())
        for value in ("😀" * 129, "😀" * 128 + "x"):
            with self.assertRaises(IllegalArgumentException):
                TagValue("tag", value)

    def test_value_and_filter_validation(self):
        for name, value in (
            (None, None),
            (" ", None),
            ("tag", " "),
            ("tag", "x" * 257),
        ):
            with self.subTest(name=name, value=value):
                with self.assertRaises(IllegalArgumentException):
                    TagValue(name, value)
        for kwargs in (
            {"types": None},
            {"types": [None]},
            {"types": [MetadataObject.Type.CATALOG]},
            {"domains": None},
            {"domains": [None]},
            {"domains": [" "]},
            {"name_contains": " "},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(IllegalArgumentException):
                    DomainMemberFilter(**kwargs)
        scopes = ["a", "b", "a"]
        member_filter = DomainMemberFilter(domains=scopes)
        scopes.clear()
        self.assertEqual(("a", "b"), member_filter.domains)
        with self.assertRaises(FrozenInstanceError):
            member_filter.name_contains = "changed"

    def test_dto_serialization_and_request_validation(self):
        data = self._dto(
            relations=[
                {
                    "relation": "via_subdomain",
                    "source": {"fullName": "child.with.dots", "type": "domain"},
                }
            ]
        )
        dto = DomainDTO.from_json(json.dumps(data))
        self.assertEqual("child.with.dots", dto.relations()[0].source().name())
        self.assertEqual(TagValue("department", "finance"), dto.backing_tag())
        self.assertEqual(dto, DomainDTO.from_json(dto.to_json()))
        self.assertEqual(
            "child.with.dots",
            MetadataObjects.parse("child.with.dots", MetadataObject.Type.DOMAIN).name(),
        )
        self.assertIsNone(
            MetadataObjectDTO(MetadataObject.Type.DOMAIN, "child.with.dots").parent()
        )
        requests = (
            DomainCreateRequest("name", None, {}, None, BackingTagCreate("tag")),
            DomainParentSetRequest("parent"),
            DomainSetRequest(Domain.Status.PUBLISHED),
            DomainBackingTagSetRequest(None),
            DomainUpdatesRequest.from_changes(
                (
                    DomainChange.rename("new"),
                    DomainChange.update_comment("comment"),
                    DomainChange.set_property("p", "v"),
                    DomainChange.remove_property("p"),
                )
            ),
        )
        for request in requests:
            request.validate()
            self.assertIsInstance(json.loads(request.to_json()), dict)
        for response in (
            DomainNamesListResponse(0, None),
            DomainListResponse(0, None),
            DomainResponse.from_dict({"code": 0, "domain": None}),
        ):
            with self.assertRaises(IllegalArgumentException):
                response.validate()
        for updates in (None, [None]):
            with self.assertRaises(IllegalArgumentException):
                DomainUpdatesRequest(updates).validate()

    def test_client_exposes_metalake_domain_operations(self):
        with patch.object(GravitinoClient, "__init__", return_value=None):
            client = GravitinoClient("http://localhost:8090", self.METALAKE)
        with patch.object(client, "get_metalake", return_value=self.metalake):
            self._respond({"code": 0, "names": [self.NAME]})
            self.assertEqual([self.NAME], client.list_domains())
            self._respond({"code": 0, "domains": [self._dto()]})
            self.assertEqual(self.NAME, client.list_domains_info()[0].name())
            self._respond({"code": 0, "domain": self._dto()})
            self.assertEqual(self.NAME, client.get_domain(self.NAME).name())
            self.assertEqual(
                self.NAME,
                client.create_domain(self.NAME, None, {}, TagValue("tag"), True).name(),
            )
            self.assertEqual(
                self.NAME,
                client.alter_domain(
                    self.NAME, DomainChange.update_comment("comment")
                ).name(),
            )
            self._respond({"code": 0, "dropped": True})
            self.assertTrue(client.delete_domain(self.NAME))

    def test_domain_changes_and_default_resource_support(self):

        for resource_type in (Fileset, Model, Table, View, SemanticModel):
            with self.subTest(resource_type=resource_type):
                with self.assertRaises(UnsupportedOperationException):
                    resource_type.supports_domains(MagicMock())
        changes = (
            DomainChange.rename("new"),
            DomainChange.update_comment(None),
            DomainChange.set_property("key", "value"),
            DomainChange.remove_property("key"),
        )
        updates = DomainUpdatesRequest.from_changes(changes)
        requests = updates._updates  # pylint: disable=protected-access
        for change, update in zip(changes, requests):
            self.assertEqual(change, update.domain_change())
            self.assertIn("DOMAIN", str(change))
        domain = MetadataObjects.parse("child.with.dots", MetadataObject.Type.DOMAIN)
        self.assertIsNone(MetadataObjects.parent(domain))
