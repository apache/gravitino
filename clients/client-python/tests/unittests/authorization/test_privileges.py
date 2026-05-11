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

from __future__ import annotations

import unittest

from gravitino.api.authorization.privileges import (
    ApplyPolicy,
    ApplyTag,
    ConsumeTopic,
    CreateCatalog,
    CreateFileset,
    CreatePolicy,
    CreateRole,
    CreateSchema,
    CreateTable,
    CreateTag,
    CreateTopic,
    CreateView,
    ExecuteFunction,
    LinkModelVersion,
    ManageGrants,
    ManageGroups,
    ManageUsers,
    ModifyFunction,
    ModifyTable,
    Privilege,
    Privileges,
    ProduceTopic,
    ReadFileset,
    RegisterFunction,
    RegisterJobTemplate,
    RegisterModel,
    RunJob,
    SelectTable,
    SelectView,
    UseCatalog,
    UseJobTemplate,
    UseModel,
    UseSchema,
    WriteFileset,
)
from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import IllegalArgumentException


class TestPrivileges(unittest.TestCase):
    def test_allow_and_deny_return_instances_for_all_privileges(self) -> None:
        for name in Privilege.Name:
            with self.subTest(privilege=name.name):
                allow_privilege = Privileges.allow(name.name)
                deny_privilege = Privileges.deny(name.name)

                self.assertEqual(name, allow_privilege.name())
                self.assertEqual(Privilege.Condition.ALLOW, allow_privilege.condition())
                self.assertIs(allow_privilege, Privileges.allow(name.name))

                self.assertEqual(name, deny_privilege.name())
                self.assertEqual(Privilege.Condition.DENY, deny_privilege.condition())
                self.assertIs(deny_privilege, Privileges.deny(name.name))

    def test_allow_raises_illegal_argument_exception_for_unknown_privilege(
        self,
    ) -> None:
        with self.assertRaises(IllegalArgumentException):
            Privileges.allow("NO_SUCH_PRIVILEGE")

    def test_deny_raises_illegal_argument_exception_for_unknown_privilege(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            Privileges.deny("NO_SUCH_PRIVILEGE")

    def test_create_catalog_binding(self) -> None:
        allow_privilege = CreateCatalog.allow()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    allow_privilege.can_bind_to(obj_type),
                )

    def test_use_catalog_binding(self) -> None:
        deny_privilege = UseCatalog.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type
                    in [MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG],
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_use_schema_binding(self) -> None:
        allow_privilege = UseSchema.allow()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    allow_privilege.can_bind_to(obj_type),
                )

    def test_create_schema_binding(self) -> None:
        deny_privilege = CreateSchema.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type
                    in [MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG],
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_table_binding(self) -> None:
        deny_privilege = CreateTable.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_select_table_binding(self) -> None:
        deny_privilege = SelectTable.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.TABLE_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_modify_table_binding(self) -> None:
        deny_privilege = ModifyTable.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.TABLE_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_fileset_binding(self) -> None:
        deny_privilege = CreateFileset.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_read_fileset_binding(self) -> None:
        deny_privilege = ReadFileset.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.FILESET_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_write_fileset_binding(self) -> None:
        deny_privilege = WriteFileset.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.FILESET_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_topic_binding(self) -> None:
        deny_privilege = CreateTopic.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_consume_topic_binding(self) -> None:
        deny_privilege = ConsumeTopic.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.TOPIC_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_produce_topic_binding(self) -> None:
        deny_privilege = ProduceTopic.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.TOPIC_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_manage_users_binding(self) -> None:
        deny_privilege = ManageUsers.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_manage_groups_binding(self) -> None:
        deny_privilege = ManageGroups.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_role_binding(self) -> None:
        deny_privilege = CreateRole.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_manage_grants_binding(self) -> None:
        deny_privilege = ManageGrants.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.MANAGE_GRANTS_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_register_model_binding(self) -> None:
        deny_privilege = RegisterModel.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_use_model_binding(self) -> None:
        deny_privilege = UseModel.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.MODEL_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_link_model_version_binding(self) -> None:
        deny_privilege = LinkModelVersion.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.MODEL_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_tag_binding(self) -> None:
        deny_privilege = CreateTag.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_apply_tag_binding(self) -> None:
        deny_privilege = ApplyTag.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in [MetadataObject.Type.METALAKE, MetadataObject.Type.TAG],
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_policy_binding(self) -> None:
        deny_privilege = CreatePolicy.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_run_job_binding(self) -> None:
        deny_privilege = RunJob.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_apply_policy_binding(self) -> None:
        deny_privilege = ApplyPolicy.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type
                    in [MetadataObject.Type.METALAKE, MetadataObject.Type.POLICY],
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_register_job_template_binding(self) -> None:
        deny_privilege = RegisterJobTemplate.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type == MetadataObject.Type.METALAKE,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_use_job_template_binding(self) -> None:
        deny_privilege = UseJobTemplate.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type
                    in [
                        MetadataObject.Type.METALAKE,
                        MetadataObject.Type.JOB_TEMPLATE,
                    ],
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_create_view_binding(self) -> None:
        deny_privilege = CreateView.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_select_view_binding(self) -> None:
        deny_privilege = SelectView.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.VIEW_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_register_function_binding(self) -> None:
        deny_privilege = RegisterFunction.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.SCHEMA_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_execute_function_binding(self) -> None:
        deny_privilege = ExecuteFunction.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.FUNCTION_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )

    def test_modify_function_binding(self) -> None:
        deny_privilege = ModifyFunction.deny()
        for obj_type in MetadataObject.Type:
            with self.subTest(obj_type=obj_type.name):
                self.assertEqual(
                    obj_type in Privileges.FUNCTION_SUPPORTED_TYPES,
                    deny_privilege.can_bind_to(obj_type),
                )
