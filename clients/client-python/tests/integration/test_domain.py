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

import os
import shutil
import tempfile
import uuid
from unittest.mock import patch

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, NameIdentifier
from gravitino.api.authorization.owner import Owner
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.domain import (
    Domain,
    DomainChange,
    DomainMemberFilter,
    DomainRelation,
)
from gravitino.api.file.fileset import Fileset
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.rel.column import Column
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.types.types import Types
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.api.tag import TagValue
from gravitino.api.tag.tag_change import TagChange
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.client.metadata_object_domain_operations import (
    MetadataObjectDomainOperations,
)
from gravitino.exceptions.base import (
    BackingTagAlreadyBoundException,
    DomainAlreadyExistsException,
    ForbiddenException,
    IllegalArgumentException,
    NoSuchDomainException,
    NoSuchTagException,
    NonEmptyDomainException,
    TagAlreadyExistsException,
)

from tests.integration.integration_test_env import IntegrationTestEnv
from tests.integration.domain_api_edges_mixin import DomainApiEdgesMixin


class TestDomain(DomainApiEdgesMixin, IntegrationTestEnv):
    """Domain SDK tests using the standard Python integration server lifecycle."""

    AUTHORIZATION = False

    def setUp(self):
        self.uri = "http://localhost:8090"
        self.authorization = self.AUTHORIZATION
        self.admin = GravitinoAdminClient(
            self.uri, auth_data_provider=SimpleAuthProvider()
        )
        self.addCleanup(self.admin.close)
        self.name = "python_domain_" + uuid.uuid4().hex[:12]
        self.admin.create_metalake(self.name, "", {})
        self.addCleanup(self.admin.drop_metalake, self.name, True)
        self.lake = GravitinoClient(
            self.uri, self.name, auth_data_provider=SimpleAuthProvider()
        )
        self.addCleanup(self.lake.close)
        self.directory = tempfile.mkdtemp(prefix="gravitino-domain-it-")
        self.addCleanup(shutil.rmtree, self.directory)

    def tearDown(self):
        # addCleanup owns this suite's clients and UUID metalake in both server modes.
        pass

    def _domain(self, name):
        return self.lake.create_domain(
            name, None, {}, TagValue(name.replace(".", "_") + " tag"), True
        )

    def _client(self, user):
        self.lake.add_user(user)
        with patch.dict(os.environ, {"GRAVITINO_USER": user}):
            client = GravitinoClient(
                self.uri, self.name, auth_data_provider=SimpleAuthProvider()
            )
        self.addCleanup(client.close)
        return client

    def _grant(self, user, objects):
        role = "role_" + uuid.uuid4().hex[:10]
        self.lake.create_role(role, securable_objects=objects)
        self.lake.grant_roles_to_user([role], user)
        return role

    @staticmethod
    def _scope(name, privileges):
        return SecurableObjects.parse(name, MetadataObject.Type.DOMAIN, privileges)

    def _relational_table(self):
        # This JDBC URI belongs to Iceberg, not to the server metadata-store backend.
        catalog = self.lake.create_catalog(
            "ice",
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "",
            {
                "catalog-backend": "jdbc",
                "warehouse": self.directory + "/warehouse",
                "uri": "jdbc:h2:mem:" + self.name + ";DB_CLOSE_DELAY=-1;MODE=MYSQL",
                "jdbc-driver": "org.h2.Driver",
                "jdbc-initialize": "true",
            },
        )
        catalog.as_schemas().create_schema("schema", "", {})
        columns = [Column.of("id", Types.LongType.get())]
        table = catalog.as_table_catalog().create_table(
            NameIdentifier.of("schema", "orders"), columns, properties={}
        )
        return catalog, table

    def _catalogs(self):
        catalog, table = self._relational_table()
        columns = [Column.of("id", Types.LongType.get())]
        hidden = catalog.as_table_catalog().create_table(
            NameIdentifier.of("schema", "hidden"), columns, properties={}
        )
        view = catalog.as_view_catalog().create_view(
            NameIdentifier.of("schema", "orders_view"),
            columns,
            [SQLRepresentation("spark", "SELECT id FROM ice.schema.orders")],
        )
        semantic = catalog.as_semantic_model_catalog().create_semantic_model(
            NameIdentifier.of("schema", "business"),
            "",
            SemanticModelDefinition(
                [Dataset("orders", NameIdentifier.of("ice", "schema", "orders"))]
            ),
            {},
        )

        # Include independent fileset and model catalogs in the same domain scope.
        files = self.lake.create_catalog(
            "files",
            Catalog.Type.FILESET,
            "hadoop",
            "",
            {"location": self.directory + "/files"},
        )
        files.as_schemas().create_schema("schema", "", {})
        fileset = files.as_fileset_catalog().create_fileset(
            NameIdentifier.of("schema", "data"),
            "",
            Fileset.Type.EXTERNAL,
            self.directory + "/external",
            {},
        )

        models = self.lake.create_catalog("models", Catalog.Type.MODEL, "model", "", {})
        models.as_schemas().create_schema("schema", "", {})
        model = models.as_model_catalog().register_model(
            NameIdentifier.of("schema", "model"), "", {}
        )
        return catalog, table, hidden, view, semantic, fileset, model

    def test_lifecycle_and_snapshots(self):
        name = "Finance_US_East"
        draft = self._domain(name)
        self.assertEqual([name], self.lake.list_domains())
        self.assertEqual(name, self.lake.list_domains_info()[0].name())
        self.assertEqual(name, self.lake.get_domain(name).name())

        # Mutations return new snapshots; the original draft retains its loaded state.
        published = draft.publish()
        self.assertEqual(Domain.Status.PUBLISHED, published.status())
        self.assertEqual(Domain.Status.DRAFT, draft.status())
        self.assertEqual(Domain.Status.DRAFT, published.unpublish().status())

        renamed = self.lake.alter_domain(
            name,
            DomainChange.rename("Renamed_US_East"),
            DomainChange.update_comment("new"),
            DomainChange.set_property("key", "value"),
        )
        self.assertEqual({"key": "value"}, renamed.properties())
        self.assertIsNone(draft.comment())
        self.assertEqual({}, draft.properties())
        self.assertEqual(
            TagValue(name.replace(".", "_") + " tag"), renamed.backing_tag()
        )
        self.assertEqual(
            {},
            self.lake.alter_domain(
                renamed.name(), DomainChange.remove_property("key")
            ).properties(),
        )

        self.assertIsNone(
            self.lake.alter_domain(
                renamed.name(), DomainChange.update_comment(None)
            ).comment()
        )
        self.assertEqual("new", renamed.comment())

        # The snapshot keeps its name, so reusing that name redirects future mutations.
        with self.assertRaises(NoSuchDomainException):
            draft.publish()
        replacement = self.lake.create_domain(
            name, None, None, TagValue("replacement"), True
        )
        self.assertEqual(Domain.Status.PUBLISHED, draft.publish().status())
        self.assertEqual(Domain.Status.DRAFT, replacement.status())
        self.assertTrue(self.lake.delete_domain(name))
        self.assertFalse(self.lake.delete_domain(name))

    def test_hierarchy_and_backing_tag_errors(self):
        parent = self._domain("Parent_with_spaces")
        other = self._domain("Other_parent_name")
        child = parent.create_subdomain(
            "Tax_EU_child", None, None, TagValue("child tag"), True
        )
        self.assertEqual([child.name()], parent.list_subdomains())
        self.assertEqual(child.name(), parent.list_subdomains_info()[0].name())

        # Publication and deletion must respect the child relationship.
        with self.assertRaises(NonEmptyDomainException):
            self.lake.delete_domain(parent.name())
        with self.assertRaises(IllegalArgumentException):
            child.publish()
        parent.publish()
        published_child = child.publish()
        with self.assertRaises(IllegalArgumentException):
            parent.unpublish()
        with self.assertRaises(IllegalArgumentException):
            published_child.attach_to_parent(other.name())

        # Move and detach use current server state while the loaded parent stays unchanged.
        other.publish()
        self.assertEqual(other.name(), child.attach_to_parent(other.name()).parent())
        self.assertEqual(parent.name(), child.parent())
        self.assertEqual([], parent.list_subdomains())
        self.assertIsNone(child.detach_from_parent().parent())
        self.assertEqual(parent.name(), child.parent())

        # Binding changes preserve tags and reject conflicting assignments.
        self.lake.create_tag("new tag", None, {}, ["finance", "risk"])
        self.assertEqual(
            TagValue("new tag", "finance"),
            child.update_backing_tag(TagValue("new tag", "finance")).backing_tag(),
        )
        conflict = self.lake.create_domain(
            "conflict", None, None, TagValue("new tag", "risk"), False
        )
        with self.assertRaises(BackingTagAlreadyBoundException):
            conflict.update_backing_tag(TagValue("new tag", "finance"))
        with self.assertRaises(NoSuchTagException):
            child.update_backing_tag(TagValue("missing"))
        self.assertIsNone(child.update_backing_tag(None).backing_tag())
        self.assertEqual(TagValue("child tag"), child.backing_tag())
        with self.assertRaises(TagAlreadyExistsException):
            self.lake.create_domain("new", None, {}, TagValue("child tag"), True)
        with self.assertRaises(DomainAlreadyExistsException):
            self.lake.create_domain(parent.name(), None, {}, TagValue("unused"), True)

    def test_members_relations_filters_and_nearest_assignment(self):
        catalog, table, hidden, view, semantic, fileset, model = self._catalogs()
        self.lake.create_tag("department", None, {}, ["finance", "risk"])
        finance = self.lake.create_domain(
            "Finance_US_East", None, {}, TagValue("department", "finance"), False
        )
        risk = self.lake.create_domain(
            "Risk_EU", None, {}, TagValue("department", "risk"), False
        )
        child = finance.create_subdomain(
            "Trading_US_with_spaces", None, {}, TagValue("trading"), True
        )

        # The schema grants Finance membership; the table assignment takes precedence for Risk.
        catalog.as_schemas().load_schema("schema").supports_tags().assign_tags(
            [{"name": "department", "value": "finance"}]
        )
        table.supports_tags().assign_tags(["trading"])
        hidden.supports_tags().assign_tags([{"name": "department", "value": "risk"}])
        fileset.supports_tags().assign_tags(
            [{"name": "department", "value": "finance"}]
        )
        model.supports_tags().assign_tags([{"name": "department", "value": "finance"}])

        # The same table has both inherited and subdomain relations, with no precedence.
        members = {
            m.object().full_name(): m
            for m in finance.list_members(DomainMemberFilter())
        }
        self.assertEqual(
            {
                "ice.schema.orders",
                "ice.schema.orders_view",
                "ice.schema.business",
                "files.schema.data",
                "models.schema.model",
            },
            set(members),
        )
        self.assertEqual(
            {DomainRelation.Type.INHERITED, DomainRelation.Type.VIA_SUBDOMAIN},
            {r.type() for r in members["ice.schema.orders"].relations()},
        )
        inherited = next(
            r
            for r in members["ice.schema.orders"].relations()
            if r.type() == DomainRelation.Type.INHERITED
        )
        self.assertEqual("ice.schema", inherited.source().full_name())
        self.assertEqual(
            DomainRelation.Type.DIRECT,
            members["files.schema.data"].relations()[0].type(),
        )
        self.assertEqual(
            ["ice.schema.hidden"],
            [m.object().full_name() for m in risk.list_members(DomainMemberFilter())],
        )

        # Reverse lookup must expose the same domain on every supported leaf resource.
        for resource in (table, view, semantic, fileset, model):
            self.assertIn(finance.name(), resource.supports_domains().list_domains())
            self.assertIn(
                finance.name(),
                [d.name() for d in resource.supports_domains().list_domains_info()],
            )

        # Scope names contain commas; repeated names select a union rather than a CSV list.
        self.assertEqual(
            ["ice.schema.orders"],
            [
                m.object().full_name()
                for m in finance.list_members(
                    DomainMemberFilter(domains=[child.name(), "unknown_scope"])
                )
            ],
        )
        self.assertEqual(
            5,
            len(
                finance.list_members(
                    DomainMemberFilter(domains=[finance.name(), child.name()])
                )
            ),
        )
        self.assertEqual(
            {MetadataObject.Type.VIEW, MetadataObject.Type.MODEL},
            {
                m.object().type()
                for m in finance.list_members(
                    DomainMemberFilter(
                        types=[MetadataObject.Type.VIEW, MetadataObject.Type.MODEL]
                    )
                )
            },
        )
        self.assertEqual(
            2, len(finance.list_members(DomainMemberFilter(name_contains="ORDERS")))
        )
        self.assertEqual(
            [], finance.list_members(DomainMemberFilter(name_contains="/ ?&+%,"))
        )
        self.assertEqual(
            [], finance.list_members(DomainMemberFilter(domains=["unknown_scope"]))
        )

        # Hierarchy and domain-name changes must appear without retagging any object.
        new_parent = self._domain("new_parent")
        child.attach_to_parent(new_parent.name())
        self.assertNotIn(new_parent.name(), view.supports_domains().list_domains())
        self.assertIn(new_parent.name(), table.supports_domains().list_domains())
        self.lake.alter_domain(child.name(), DomainChange.rename("renamed_child"))
        self.assertIn("renamed_child", table.supports_domains().list_domains())
        self.lake.delete_domain("renamed_child")
        self.assertNotIn(new_parent.name(), table.supports_domains().list_domains())
        self.assertEqual("trading", self.lake.get_tag("trading").name())
        finance.update_backing_tag(None)
        self.assertEqual([], table.supports_domains().list_domains())

    def test_authorization_visibility_inheritance_denies_and_ownership(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        parent = self._domain("Parent_auth_scope")
        child = parent.create_subdomain(
            "Child_auth_scope", None, {}, TagValue("child"), True
        )
        reader = self._client("reader")
        manager = self._client("manager")
        outsider = self._client("outsider")
        owner = self._client("domain_owner")
        self._grant(
            "reader", [self._scope(parent.name(), [Privileges.allow("USE_DOMAIN")])]
        )
        self._grant(
            "manager", [self._scope(parent.name(), [Privileges.allow("MANAGE_DOMAIN")])]
        )

        # USE_DOMAIN cannot reveal drafts; inherited MANAGE_DOMAIN can.
        self.assertEqual([], reader.list_domains())
        with self.assertRaises(NoSuchDomainException):
            reader.get_domain(parent.name())
        self.assertEqual(child.name(), manager.get_domain(child.name()).name())

        # Publication reveals one level at a time to the reader.
        manager.get_domain(parent.name()).publish()
        self.assertEqual([parent.name()], reader.list_domains())
        with self.assertRaises(NoSuchDomainException):
            reader.get_domain(child.name())
        manager.get_domain(child.name()).publish()
        self.assertEqual({parent.name(), child.name()}, set(reader.list_domains()))
        self.assertEqual([], outsider.list_domains())
        with self.assertRaises(ForbiddenException):
            outsider.get_domain(parent.name())
        with self.assertRaises(ForbiddenException):
            reader.get_domain(parent.name()).unpublish()

        # A child deny overrides a grant inherited from the parent.
        self._grant(
            "reader", [self._scope(child.name(), [Privileges.deny("USE_DOMAIN")])]
        )
        self.assertEqual([parent.name()], reader.list_domains())
        with self.assertRaises(ForbiddenException):
            reader.get_domain(child.name())

        # Owners bypass denies, and metalake grants also apply to child domains.
        self.lake.set_owner(
            MetadataObjects.parse(parent.name(), MetadataObject.Type.DOMAIN),
            "domain_owner",
            Owner.Type.USER,
        )
        self._grant(
            "domain_owner",
            [
                self._scope(
                    child.name(),
                    [Privileges.deny("USE_DOMAIN"), Privileges.deny("MANAGE_DOMAIN")],
                )
            ],
        )
        self.assertEqual(child.name(), owner.get_domain(child.name()).name())
        self.assertEqual(
            Domain.Status.DRAFT, owner.get_domain(child.name()).unpublish().status()
        )
        inherited = self._client("lake_manager")
        self._grant(
            "lake_manager",
            [
                SecurableObjects.of_metalake(
                    self.name, [Privileges.allow("MANAGE_DOMAIN")]
                )
            ],
        )
        self.assertEqual(child.name(), inherited.get_domain(child.name()).name())

    def test_authorization_backing_tag_creation_and_binding(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        creator = self._client("creator")
        self._grant(
            "creator",
            [
                SecurableObjects.of_metalake(
                    self.name, [Privileges.allow("MANAGE_DOMAIN")]
                )
            ],
        )
        with self.assertRaises(ForbiddenException):
            creator.create_domain("denied", None, {}, TagValue("new tag"), True)
        self._grant(
            "creator",
            [SecurableObjects.of_metalake(self.name, [Privileges.allow("CREATE_TAG")])],
        )

        # Creating a backing tag needs CREATE_TAG in addition to domain management.
        created = creator.create_domain(
            "created", None, {}, TagValue("owned tag"), True
        )

        # Managing a domain does not confer ownership of another user's tag.
        self.lake.create_tag("foreign tag", None, {})
        with self.assertRaises(ForbiddenException):
            created.update_backing_tag(TagValue("foreign tag"))
        with self.assertRaises(ForbiddenException):
            creator.create_domain("foreign", None, {}, TagValue("foreign tag"), False)

        self.lake.set_owner(
            MetadataObjects.parse("foreign tag", MetadataObject.Type.TAG),
            "creator",
            Owner.Type.USER,
        )
        bound = creator.create_domain("bound", None, {}, TagValue("foreign tag"), False)
        self.assertEqual(TagValue("foreign tag"), bound.backing_tag())
        self.assertIsNone(bound.update_backing_tag(None).backing_tag())
        self.assertEqual(
            TagValue("foreign tag"),
            created.update_backing_tag(TagValue("foreign tag")).backing_tag(),
        )
        with self.assertRaises(BackingTagAlreadyBoundException):
            creator.create_domain("conflict", None, {}, TagValue("foreign tag"), False)
        with self.assertRaises(NoSuchTagException):
            creator.create_domain("missing", None, {}, TagValue("missing tag"), False)

    def test_authorization_member_access_without_tag_privileges(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        catalog, table, _, _, _, _, _ = self._catalogs()
        parent = self._domain("access_parent")
        child = parent.create_subdomain(
            "access_child", None, {}, TagValue("child tag"), True
        )
        parent.publish()
        catalog.as_schemas().load_schema("schema").supports_tags().assign_tags(
            [parent.backing_tag().name()]
        )
        table.supports_tags().assign_tags(["child tag"])

        # Grant access to one table, without granting any tag privileges.
        reader = self._client("object_reader")
        cat_scope = SecurableObjects.of_catalog(
            "ice", [Privileges.allow("USE_CATALOG")]
        )
        schema_scope = SecurableObjects.of_schema(
            SecurableObjects.of_catalog("ice", []),
            "schema",
            [Privileges.allow("USE_SCHEMA")],
        )
        table_scope = SecurableObjects.of_table(
            schema_scope, "orders", [Privileges.allow("SELECT_TABLE")]
        )
        self._grant(
            "object_reader",
            [
                self._scope(parent.name(), [Privileges.allow("USE_DOMAIN")]),
                cat_scope,
                schema_scope,
                table_scope,
            ],
        )

        visible = reader.get_domain(parent.name()).list_members(DomainMemberFilter())
        self.assertEqual(
            ["ice.schema.orders"], [m.object().full_name() for m in visible]
        )
        self.assertEqual(
            [DomainRelation.Type.INHERITED], [r.type() for r in visible[0].relations()]
        )

        self.assertEqual("ice.schema", visible[0].relations()[0].source().full_name())
        self.assertEqual([], reader.get_domain(parent.name()).list_subdomains())
        self.assertEqual([], reader.get_domain(parent.name()).list_subdomains_info())
        self.assertEqual(
            [parent.name()], [d.name() for d in reader.list_domains_info()]
        )
        self.assertEqual(
            [],
            reader.get_domain(parent.name()).list_members(
                DomainMemberFilter(domains=[child.name()])
            ),
        )

        # A hidden child contributes no roll-up relation until it is published.
        child.publish()
        visible = reader.get_domain(parent.name()).list_members(DomainMemberFilter())
        self.assertEqual(
            {DomainRelation.Type.INHERITED, DomainRelation.Type.VIA_SUBDOMAIN},
            {r.type() for r in visible[0].relations()},
        )
        lookup = MetadataObjectDomainOperations(
            self.name,
            MetadataObjects.parse("ice.schema.orders", MetadataObject.Type.TABLE),
            reader.get_metalake().rest_client,
        )
        self.assertEqual({parent.name(), child.name()}, set(lookup.list_domains()))

        # Denying the child removes its relation while preserving inherited parent membership.
        self._grant(
            "object_reader",
            [self._scope(child.name(), [Privileges.deny("USE_DOMAIN")])],
        )
        parent_only = reader.get_domain(parent.name()).list_members(
            DomainMemberFilter()
        )
        self.assertEqual(1, len(parent_only))
        self.assertEqual(
            [DomainRelation.Type.INHERITED],
            [r.type() for r in parent_only[0].relations()],
        )
        self.assertEqual([parent.name()], lookup.list_domains())
        self.assertEqual(1, len(lookup.list_domains_info()[0].relations()))

        # Revoking object access removes membership and blocks reverse lookup.
        self._grant(
            "object_reader",
            [
                SecurableObjects.of_table(
                    schema_scope, "orders", [Privileges.deny("SELECT_TABLE")]
                )
            ],
        )
        self.assertEqual(
            [], reader.get_domain(parent.name()).list_members(DomainMemberFilter())
        )
        with self.assertRaises(ForbiddenException):
            lookup.list_domains()

    def test_authorization_move_rechecks_current_parent_and_metalake_owner(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        first = self._domain("first").publish()
        second = self._domain("second").publish()
        child = first.create_subdomain(
            "moving_child_name", None, {}, TagValue("moving tag"), True
        ).publish()
        manager = self._client("moving_manager")
        owner = self._client("lake_owner")
        self._grant(
            "moving_manager",
            [self._scope(first.name(), [Privileges.allow("MANAGE_DOMAIN")])],
        )
        snapshot = manager.get_domain(child.name())
        with self.assertRaises(ForbiddenException):
            snapshot.attach_to_parent(second.name())

        # The old parent grant must stop authorizing mutations after the move.
        child.attach_to_parent(second.name())
        with self.assertRaises(ForbiddenException):
            snapshot.unpublish()

        self._grant(
            "moving_manager",
            [self._scope(second.name(), [Privileges.allow("MANAGE_DOMAIN")])],
        )
        self.assertEqual(Domain.Status.DRAFT, snapshot.unpublish().status())
        self.assertEqual(first.name(), snapshot.parent())
        self._grant(
            "moving_manager",
            [self._scope(child.name(), [Privileges.deny("MANAGE_DOMAIN")])],
        )
        with self.assertRaises(NoSuchDomainException):
            manager.get_domain(child.name())
        self._grant(
            "lake_owner",
            [
                self._scope(
                    child.name(),
                    [Privileges.deny("MANAGE_DOMAIN"), Privileges.deny("USE_DOMAIN")],
                )
            ],
        )

        # Restore ownership even on assertion failure so the fixture can delete its metalake.
        root = MetadataObjects.parse(self.name, MetadataObject.Type.METALAKE)
        self.lake.set_owner(root, "lake_owner", Owner.Type.USER)
        try:
            self.assertEqual(child.name(), owner.get_domain(child.name()).name())
        finally:
            owner.set_owner(root, "anonymous", Owner.Type.USER)

    @staticmethod
    def _relation_keys(relations):
        return {
            (
                relation.type(),
                relation.source().type() if relation.source() else None,
                relation.source().full_name() if relation.source() else None,
            )
            for relation in relations
        }

    def _assert_table_relations(self, domain, table, expected):
        members = domain.list_members(
            DomainMemberFilter(
                types=[MetadataObject.Type.TABLE], name_contains="ice.schema.orders"
            )
        )
        reverse = [
            candidate
            for candidate in table.supports_domains().list_domains_info()
            if candidate.name() == domain.name()
        ]
        self.assertEqual(
            bool(expected), domain.name() in table.supports_domains().list_domains()
        )
        if not expected:
            self.assertEqual([], members)
            self.assertEqual([], reverse)
            return

        self.assertEqual(1, len(members))
        self.assertEqual("ice.schema.orders", members[0].object().full_name())
        self.assertEqual(MetadataObject.Type.TABLE, members[0].object().type())
        self.assertEqual(len(expected), len(members[0].relations()))
        self.assertEqual(expected, self._relation_keys(members[0].relations()))
        self.assertEqual(1, len(reverse))
        self.assertEqual(len(expected), len(reverse[0].relations()))
        self.assertEqual(expected, self._relation_keys(reverse[0].relations()))
        self.assertEqual([], domain.relations())
        for relation in members[0].relations():
            if relation.type() == DomainRelation.Type.VIA_SUBDOMAIN:
                self.assertEqual(
                    relation.source().full_name(), relation.source().name()
                )
                self.assertIsNone(relation.source().parent())

    def test_nearest_tag_assignment_and_value_matching(self):
        catalog, table = self._relational_table()
        self.lake.create_tag("department", None, {}, ["finance", "risk", "Finance"])
        finance = self.lake.create_domain(
            "Finance", None, {}, TagValue("department", "finance"), False
        )
        risk = self.lake.create_domain(
            "Risk", None, {}, TagValue("department", "risk"), False
        )
        capitalized = self.lake.create_domain(
            "Capitalized", None, {}, TagValue("department", "Finance"), False
        )
        inherited_catalog = (
            DomainRelation.Type.INHERITED,
            MetadataObject.Type.CATALOG,
            "ice",
        )
        inherited_schema = (
            DomainRelation.Type.INHERITED,
            MetadataObject.Type.SCHEMA,
            "ice.schema",
        )
        direct = (DomainRelation.Type.DIRECT, None, None)
        schema_tags = catalog.as_schemas().load_schema("schema").supports_tags()

        with self.assertRaises(IllegalArgumentException):
            finance.update_backing_tag(TagValue("department", "unknown"))
        self.assertEqual(
            TagValue("department", "finance"),
            self.lake.get_domain(finance.name()).backing_tag(),
        )

        # A nearer schema assignment masks the catalog's value of the same tag.
        catalog.supports_tags().assign_tags(
            [{"name": "department", "value": "finance"}]
        )
        self._assert_table_relations(finance, table, {inherited_catalog})
        self._assert_table_relations(capitalized, table, set())
        schema_tags.assign_tags([{"name": "department", "value": "risk"}])
        self._assert_table_relations(finance, table, set())
        self._assert_table_relations(risk, table, {inherited_schema})

        # Each direct value matches exactly; remaining values still mask inherited assignments.
        table.supports_tags().assign_tags(
            [
                {"name": "department", "value": "finance"},
                {"name": "department", "value": "Finance"},
            ]
        )
        self._assert_table_relations(finance, table, {direct})
        self._assert_table_relations(capitalized, table, {direct})
        self._assert_table_relations(risk, table, set())
        table.supports_tags().assign_tags(
            tags_to_remove=[{"name": "department", "value": "finance"}]
        )
        self._assert_table_relations(finance, table, set())
        self._assert_table_relations(capitalized, table, {direct})
        self._assert_table_relations(risk, table, set())

        # Removing assignments reveals the next ancestor, until no assignment remains.
        table.supports_tags().assign_tags(
            tags_to_remove=[{"name": "department", "value": "Finance"}]
        )
        self._assert_table_relations(risk, table, {inherited_schema})
        schema_tags.assign_tags(
            tags_to_remove=[{"name": "department", "value": "risk"}]
        )
        self._assert_table_relations(finance, table, {inherited_catalog})
        catalog.supports_tags().assign_tags(
            tags_to_remove=[{"name": "department", "value": "finance"}]
        )
        for domain in (finance, risk, capitalized):
            self._assert_table_relations(domain, table, set())

        # A backing binding without a value matches any valued or valueless assignment.
        self.lake.create_tag("any-value", None, {})
        any_value = self.lake.create_domain(
            "Any", None, {}, TagValue("any-value"), False
        )
        table.supports_tags().assign_tags([{"name": "any-value", "value": "arbitrary"}])
        self._assert_table_relations(any_value, table, {direct})
        table.supports_tags().assign_tags(
            tags_to_remove=[{"name": "any-value", "value": "arbitrary"}]
        )
        table.supports_tags().assign_tags(["any-value"])
        self._assert_table_relations(any_value, table, {direct})
        with self.assertRaises(BackingTagAlreadyBoundException):
            self.lake.create_domain(
                "Mixed", None, {}, TagValue("any-value", "arbitrary"), False
            )

    def test_multiple_subdomain_relations_and_backing_tag_identity(self):
        catalog, table = self._relational_table()
        parent = self._domain("Parent")
        first = parent.create_subdomain(
            "First_child_A", None, {}, TagValue("first-tag"), True
        )
        second = parent.create_subdomain(
            "Second_child_B", None, {}, TagValue("second-tag"), True
        )
        catalog.supports_tags().assign_tags(["first-tag"])
        catalog.as_schemas().load_schema("schema").supports_tags().assign_tags(
            ["second-tag"]
        )
        table.supports_tags().assign_tags([parent.backing_tag().name()])
        direct = (DomainRelation.Type.DIRECT, None, None)
        first_path = (
            DomainRelation.Type.VIA_SUBDOMAIN,
            MetadataObject.Type.DOMAIN,
            first.name(),
        )
        second_path = (
            DomainRelation.Type.VIA_SUBDOMAIN,
            MetadataObject.Type.DOMAIN,
            second.name(),
        )
        inherited_catalog = (
            DomainRelation.Type.INHERITED,
            MetadataObject.Type.CATALOG,
            "ice",
        )
        inherited_schema = (
            DomainRelation.Type.INHERITED,
            MetadataObject.Type.SCHEMA,
            "ice.schema",
        )

        # The member is deduplicated, while all direct and child paths remain visible.
        self._assert_table_relations(parent, table, {direct, first_path, second_path})
        self._assert_table_relations(first, table, {inherited_catalog})
        self._assert_table_relations(second, table, {inherited_schema})
        selected = parent.list_members(
            DomainMemberFilter(
                domains=[first.name(), second.name()],
                types=[MetadataObject.Type.TABLE],
                name_contains="ORDERS",
            )
        )
        self.assertEqual(1, len(selected))
        self.assertEqual(
            {direct, first_path, second_path},
            self._relation_keys(selected[0].relations()),
        )
        self.assertEqual(
            [],
            parent.list_members(
                DomainMemberFilter(
                    domains=[first.name()], types=[MetadataObject.Type.VIEW]
                )
            ),
        )
        self.assertEqual(
            [], first.list_members(DomainMemberFilter(domains=[second.name()]))
        )

        # Removing one route must preserve the other routes and the member's own child scope.
        table.supports_tags().assign_tags(tags_to_remove=[parent.backing_tag().name()])
        self._assert_table_relations(parent, table, {first_path, second_path})
        second.detach_from_parent()
        self._assert_table_relations(parent, table, {first_path})
        first = self.lake.alter_domain(
            first.name(), DomainChange.rename("Renamed_child_A")
        )
        first_path = (
            DomainRelation.Type.VIA_SUBDOMAIN,
            MetadataObject.Type.DOMAIN,
            first.name(),
        )
        self._assert_table_relations(parent, table, {first_path})
        parent = self.lake.alter_domain(
            parent.name(), DomainChange.rename("Renamed_parent_P")
        )
        self.assertEqual(parent.name(), self.lake.get_domain(first.name()).parent())
        self._assert_table_relations(parent, table, {first_path})

        # Tag identity survives rename, but deletion and same-name recreation cannot restore a binding.
        self.lake.alter_tag("first-tag", TagChange.rename("renamed-first-tag"))
        self.assertEqual(
            "renamed-first-tag", self.lake.get_domain(first.name()).backing_tag().name()
        )
        self._assert_table_relations(parent, table, {first_path})
        self.lake.delete_tag("renamed-first-tag")
        self.assertIsNone(self.lake.get_domain(first.name()).backing_tag())
        self._assert_table_relations(parent, table, set())
        self.lake.create_tag("renamed-first-tag", None, {})
        catalog.supports_tags().assign_tags(["renamed-first-tag"])
        self._assert_table_relations(first, table, set())
        self._assert_table_relations(parent, table, set())
        first.update_backing_tag(TagValue("renamed-first-tag"))
        self._assert_table_relations(first, table, {inherited_catalog})
        self._assert_table_relations(parent, table, {first_path})
        first.update_backing_tag(None)
        self._assert_table_relations(parent, table, set())

    def test_invalid_hierarchy_atomic_creation_and_idempotence(self):
        parent = self._domain("Parent")
        other = self._domain("Other")
        child = parent.create_subdomain("Child", None, {}, TagValue("child-tag"), True)

        self.lake.create_tag("existing-valued-tag", None, {}, ["finance"])
        bound = parent.create_subdomain(
            "Bound",
            "existing tag",
            {"key": "value"},
            TagValue("existing-valued-tag", "finance"),
            False,
        )
        self.assertEqual(parent.name(), bound.parent())
        self.assertEqual(
            TagValue("existing-valued-tag", "finance"), bound.backing_tag()
        )
        self.assertEqual({"key": "value"}, bound.properties())
        with self.assertRaises(DomainAlreadyExistsException):
            self.lake.alter_domain(other.name(), DomainChange.rename(parent.name()))
        self.assertEqual(other.name(), self.lake.get_domain(other.name()).name())

        # Rejected hierarchy or duplicate-name creation must not leave a domain or backing tag.
        with self.assertRaises(IllegalArgumentException):
            child.create_subdomain(
                "Grandchild", None, {}, TagValue("grandchild-tag"), True
            )
        with self.assertRaises(NoSuchDomainException):
            self.lake.get_domain("Grandchild")
        with self.assertRaises(NoSuchTagException):
            self.lake.get_tag("grandchild-tag")
        with self.assertRaises(DomainAlreadyExistsException):
            self.lake.create_domain(
                parent.name(), None, {}, TagValue("unused-tag"), True
            )
        with self.assertRaises(NoSuchTagException):
            self.lake.get_tag("unused-tag")
        for domain, target in ((child, child), (other, child), (parent, other)):
            with self.assertRaises(IllegalArgumentException):
                domain.attach_to_parent(target.name())
        self.assertEqual(parent.name(), child.attach_to_parent(parent.name()).parent())
        self.assertEqual(
            child.backing_tag(),
            child.update_backing_tag(child.backing_tag()).backing_tag(),
        )

        # Repeated state changes and detach calls are safe; old snapshots stay unchanged.
        self.assertEqual(Domain.Status.PUBLISHED, parent.publish().publish().status())
        self.assertEqual(Domain.Status.PUBLISHED, child.publish().publish().status())
        self.assertEqual(Domain.Status.DRAFT, child.unpublish().unpublish().status())
        self.assertEqual(Domain.Status.DRAFT, parent.unpublish().unpublish().status())
        self.assertIsNone(parent.detach_from_parent().parent())
        self.assertIsNone(child.detach_from_parent().detach_from_parent().parent())
        self.assertEqual(parent.name(), child.parent())
        self.assertIsNotNone(child.audit_info().creator())
        self.assertIsNotNone(child.audit_info().create_time())
