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

from gravitino.api.authorization.owner import Owner
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.domain import (
    Domain,
    DomainChange,
    DomainMemberFilter,
    DomainRelation,
)
from gravitino.api.tag import TagValue
from gravitino.exceptions.base import (
    ForbiddenException,
    IllegalArgumentException,
    NoSuchDomainException,
    NoSuchTagException,
    TagAlreadyExistsException,
)


class DomainApiEdgesMixin:
    """Endpoint matrices shared by the ordinary and authorization Domain suites."""

    def test_authorization_write_endpoint_matrix(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        parent = self._domain("matrix-parent").publish()
        child = parent.create_subdomain(
            "matrix-child", None, {}, TagValue("matrix-child-tag"), True
        ).publish()
        target = self._domain("matrix-target").publish()

        creation_parent = self._domain("creation-parent").publish()

        for user in ("matrix_reader", "matrix_denied"):
            reader = self._client(user)
            self._grant(
                user,
                [
                    SecurableObjects.of_metalake(
                        self.name,
                        [
                            Privileges.allow("USE_DOMAIN"),
                            Privileges.allow("CREATE_TAG"),
                        ],
                    )
                ],
            )
            if user == "matrix_denied":
                self._grant(
                    user,
                    [
                        SecurableObjects.of_metalake(
                            self.name, [Privileges.allow("MANAGE_DOMAIN")]
                        ),
                        self._scope(child.name(), [Privileges.deny("MANAGE_DOMAIN")]),
                        self._scope(
                            creation_parent.name(), [Privileges.deny("MANAGE_DOMAIN")]
                        ),
                    ],
                )
            snapshot = reader.get_domain(child.name())
            # Read permission remains effective despite an explicit management deny.
            self.assertEqual([], snapshot.list_members(DomainMemberFilter()))
            self.assertEqual([], snapshot.list_subdomains())
            self.assertEqual([], snapshot.list_subdomains_info())
            writes = {
                "alter": lambda client=reader: client.alter_domain(
                    child.name(), DomainChange.update_comment("denied")
                ),
                "delete": lambda client=reader: client.delete_domain(child.name()),
                "publish": snapshot.publish,
                "unpublish": snapshot.unpublish,
                "attach": lambda current=snapshot: current.attach_to_parent(
                    target.name()
                ),
                "detach": snapshot.detach_from_parent,
                "backing_tag": lambda current=snapshot: current.update_backing_tag(
                    None
                ),
                "subdomain": lambda client=reader: client.get_domain(
                    creation_parent.name()
                ).create_subdomain(
                    "denied-subdomain", None, {}, TagValue("denied-tag"), True
                ),
            }
            if user == "matrix_reader":
                writes["create"] = lambda client=reader: client.create_domain(
                    "denied-domain", None, {}, TagValue("denied-create-tag"), True
                )
            for endpoint, operation in writes.items():
                with self.subTest(user=user, endpoint=endpoint):
                    with self.assertRaises(ForbiddenException):
                        operation()

            # Rejected writes leave the cached state unchanged.
            unchanged = self.lake.get_domain(child.name())
            self.assertEqual(Domain.Status.PUBLISHED, unchanged.status())
            self.assertEqual(parent.name(), unchanged.parent())
            self.assertEqual(child.backing_tag(), unchanged.backing_tag())
            self.assertEqual(child.comment(), unchanged.comment())
            self.assertEqual([child.name()], parent.list_subdomains())
            self.assertEqual([], creation_parent.list_subdomains())
            for name in ("denied-domain", "denied-subdomain"):
                with self.assertRaises(NoSuchDomainException):
                    self.lake.get_domain(name)
            for name in ("denied-tag", "denied-create-tag"):
                with self.assertRaises(NoSuchTagException):
                    self.lake.get_tag(name)

    def test_authorization_non_owner_manager_write_endpoints(self):
        if not self.authorization:
            self.skipTest("Requires an authorization-enabled server")
        parent = self._domain("allowed-parent").publish()
        target = self._domain("allowed-target").publish()
        child = parent.create_subdomain(
            "allowed-child", None, {}, TagValue("allowed-child-tag"), True
        ).publish()
        creation_parent = self._domain("allowed-creation-parent").publish()
        manager = self._client("allowed_manager")
        self._grant(
            "allowed_manager",
            [
                SecurableObjects.of_metalake(
                    self.name,
                    [Privileges.allow("MANAGE_DOMAIN"), Privileges.allow("CREATE_TAG")],
                )
            ],
        )

        # Domain ownership stays with the admin; tag ownership is satisfied independently.
        self.lake.create_tag("manager-binding-tag", None, {})
        self.lake.set_owner(
            MetadataObjects.parse("manager-binding-tag", MetadataObject.Type.TAG),
            "allowed_manager",
            Owner.Type.USER,
        )
        managed = manager.get_domain(child.name())
        self.assertEqual(
            "managed",
            manager.alter_domain(
                child.name(), DomainChange.update_comment("managed")
            ).comment(),
        )
        self.assertEqual(Domain.Status.DRAFT, managed.unpublish().status())
        self.assertEqual(Domain.Status.PUBLISHED, managed.publish().status())
        self.assertEqual(
            target.name(), managed.attach_to_parent(target.name()).parent()
        )
        self.assertIsNone(managed.detach_from_parent().parent())
        self.assertEqual(
            TagValue("manager-binding-tag"),
            managed.update_backing_tag(TagValue("manager-binding-tag")).backing_tag(),
        )
        self.assertIsNone(managed.update_backing_tag(None).backing_tag())

        # These legal requests match the denied creation requests with management now allowed.
        created = manager.create_domain(
            "denied-domain", None, {}, TagValue("denied-create-tag"), True
        )
        subdomain = manager.get_domain(creation_parent.name()).create_subdomain(
            "denied-subdomain", None, {}, TagValue("denied-tag"), True
        )
        self.assertEqual(creation_parent.name(), subdomain.parent())
        self.assertEqual(created.name(), manager.get_domain(created.name()).name())
        self.assertTrue(manager.delete_domain(child.name()))
        with self.assertRaises(NoSuchDomainException):
            self.lake.get_domain(child.name())

    def test_backing_tag_value_length_boundaries(self):
        _, table = self._relational_table()
        for index, value in enumerate(("x" * 256, "😀" * 128)):
            with self.subTest(index=index):
                tag_name = f"unicode-boundary-{index}"
                self.lake.create_tag(tag_name, None, {})
                name = "F" * 63 + str(index)
                binding = TagValue(tag_name, value)
                domain = self.lake.create_domain(
                    name, "Unicode comment 中文😀", {"region": "亚洲😀"}, binding, False
                )
                loaded = self.lake.get_domain(name)
                self.assertEqual(name, loaded.name())
                self.assertEqual(binding, loaded.backing_tag())
                self.assertEqual({"region": "亚洲😀"}, loaded.properties())

                # Boundary values survive serialization, storage and membership resolution.
                table.supports_tags().assign_tags([{"name": tag_name, "value": value}])
                direct = {(DomainRelation.Type.DIRECT, None, None)}
                self._assert_table_relations(domain, table, direct)
                renamed = f"Tax-{index}"
                changed = self.lake.alter_domain(name, DomainChange.rename(renamed))
                self.assertEqual(renamed, self.lake.get_domain(renamed).name())
                self.assertEqual(binding, changed.backing_tag())
                self._assert_table_relations(changed, table, direct)
                with self.assertRaises(NoSuchDomainException):
                    self.lake.get_domain(name)

                # The Python client must enforce Java's UTF-16-unit limit before HTTP.
                with self.assertRaises(IllegalArgumentException):
                    changed.update_backing_tag(TagValue(tag_name, value + "x"))
                self.assertEqual(binding, self.lake.get_domain(renamed).backing_tag())
                rejected_name = f"rejected-boundary-{index}"
                with self.assertRaises(IllegalArgumentException):
                    self.lake.create_domain(
                        rejected_name, None, {}, TagValue(tag_name, value + "x"), False
                    )
                with self.assertRaises(NoSuchDomainException):
                    self.lake.get_domain(rejected_name)
                self._assert_table_relations(changed, table, direct)

    def test_real_response_value_equality_and_snapshot_protection(self):
        _, table = self._relational_table()
        created = self.lake.create_domain(
            "value-contract",
            "original",
            {"key": "value"},
            TagValue("value-contract-tag"),
            True,
        )
        loaded = self.lake.get_domain(created.name())
        listed = self.lake.list_domains_info()[0]
        self.assertEqual(created, loaded)
        self.assertEqual(loaded, listed)
        self.assertEqual(hash(created), hash(loaded))
        self.assertIn(listed, {created})
        loaded.properties().clear()
        self.assertEqual({"key": "value"}, loaded.properties())

        table.supports_tags().assign_tags(["value-contract-tag"])
        member_filter = DomainMemberFilter(types=[MetadataObject.Type.TABLE])
        member = loaded.list_members(member_filter)[0]
        reloaded = loaded.list_members(member_filter)[0]
        metadata_object = MetadataObjects.parse(
            "ice.schema.orders", MetadataObject.Type.TABLE
        )
        self.assertEqual(member, reloaded)
        self.assertEqual(hash(member), hash(reloaded))
        self.assertIn(reloaded, {member})
        self.assertEqual(metadata_object, member.object())
        self.assertEqual(hash(metadata_object), hash(member.object()))
        self.assertEqual("ice.schema", member.object().parent())

        # Modifying returned collections must not change snapshots or their set membership.
        member_hash = hash(member)
        member.relations().clear()
        self.assertEqual(
            [DomainRelation(DomainRelation.Type.DIRECT)], member.relations()
        )
        self.assertEqual(member_hash, hash(member))
        reverse = table.supports_domains().list_domains_info()[0]
        reverse_reloaded = table.supports_domains().list_domains_info()[0]
        self.assertEqual(reverse, reverse_reloaded)
        reverse_hash = hash(reverse)
        reverse.relations().clear()
        self.assertEqual(
            [DomainRelation(DomainRelation.Type.DIRECT)], reverse.relations()
        )
        self.assertEqual(reverse_hash, hash(reverse))
        self.assertIn(reverse_reloaded, {reverse})

        changed = self.lake.alter_domain(
            loaded.name(),
            DomainChange.update_comment("changed"),
            DomainChange.set_property("key", "changed"),
        )
        self.assertNotEqual(loaded, changed)
        self.assertEqual("original", loaded.comment())
        self.assertEqual({"key": "value"}, loaded.properties())
        self.assertEqual(created, loaded)

    def test_missing_domain_endpoint_matrix(self):
        removed = self._domain("removed")
        self.lake.delete_domain(removed.name())
        operations = {
            "alter": lambda: self.lake.alter_domain(
                removed.name(), DomainChange.update_comment("missing")
            ),
            "publish": removed.publish,
            "unpublish": removed.unpublish,
            "attach": lambda: removed.attach_to_parent("missing-parent"),
            "detach": removed.detach_from_parent,
            "backing_tag": lambda: removed.update_backing_tag(None),
            "subdomains": removed.list_subdomains,
            "subdomains_info": removed.list_subdomains_info,
            "members": lambda: removed.list_members(DomainMemberFilter()),
            "create_subdomain": lambda: removed.create_subdomain(
                "orphan", None, {}, TagValue("orphan-tag"), True
            ),
        }
        for endpoint, operation in operations.items():
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(NoSuchDomainException):
                    operation()
        with self.assertRaises(NoSuchTagException):
            self.lake.get_tag("orphan-tag")
        self.assertFalse(self.lake.delete_domain(removed.name()))

    def test_invalid_bindings_leave_no_partial_changes(self):
        domain = self._domain("binding-errors")
        with self.assertRaises(NoSuchDomainException):
            domain.attach_to_parent("missing-parent")
        self.assertIsNone(self.lake.get_domain(domain.name()).parent())
        with self.assertRaises(NoSuchTagException):
            domain.update_backing_tag(TagValue("absent-tag"))
        self.assertEqual(
            domain.backing_tag(), self.lake.get_domain(domain.name()).backing_tag()
        )

        # A rejected creation must not adopt an existing tag or leave an orphan domain.
        invalid = [
            ("missing-binding", TagValue("absent-tag"), False, NoSuchTagException),
            ("existing-binding", domain.backing_tag(), True, TagAlreadyExistsException),
            (
                "valued-new-tag",
                TagValue("invalid-new-tag", "value"),
                True,
                IllegalArgumentException,
            ),
        ]
        for name, tag, create_tag, error in invalid:
            with self.subTest(name=name):
                with self.assertRaises(error):
                    self.lake.create_domain(name, None, {}, tag, create_tag)
                with self.assertRaises(NoSuchDomainException):
                    self.lake.get_domain(name)
        with self.assertRaises(NoSuchTagException):
            self.lake.get_tag("invalid-new-tag")
        self.assertIsNotNone(self.lake.get_tag(domain.backing_tag().name()))
