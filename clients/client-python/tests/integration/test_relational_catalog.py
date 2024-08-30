"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import logging
from random import randint
from typing import Dict, List

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
    Catalog,
)
from gravitino.api.table import Table
from gravitino.api.rel.types.type import Type
from gravitino.api.rel.types.types import Types
from gravitino.api.rel.expressions.expression import Expression
from gravitino.dto.rel.column_dto import ColumnDTO

from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestRelationalCatalog(IntegrationTestEnv):
    metalake_name: str = "TestRelationalCatalog_metalake" + str(randint(1, 10000))

    catalog_name: str = "test_catalog"
    catalog_provider: str = "jdbc-mysql"
    catalog_properties_key1: str = "jdbc-driver"
    catalog_properties_value1: str = "com.mysql.cj.jdbc.Driver"
    catalog_properties_key2: str = "jdbc-url"
    catalog_properties_value2: str = "jdbc:mysql://mysql:3306"
    catalog_properties_key3: str = "jdbc-user"
    catalog_properties_value3: str = "mysql"
    catalog_properties_key4: str = "jdbc-password"
    catalog_properties_value4: str = "mysql"
    catalog_properties: Dict[str, str] = {
        catalog_properties_key1: catalog_properties_value1,
        catalog_properties_key2: catalog_properties_value2,
        catalog_properties_key3: catalog_properties_value3,
        catalog_properties_key4: catalog_properties_value4,
    }
    schema_name: str = "test_schema" + str(randint(1, 10000))

    table_name: str = "test_table"
    table_comment: str = "test_table_comment"
    table_properties_key1: str = "engine"
    table_properties_value1: str = "InnoDB"
    table_properties_key2: str = "auto-increment-offset"
    table_properties_value2: str = "1"
    table_properties: Dict[str, str] = {
        table_properties_key1: table_properties_value1,
        table_properties_key2: table_properties_value2,
    }
    table_new_name = table_name + "_new"

    catalog_ident: NameIdentifier = NameIdentifier.of(metalake_name, catalog_name)
    schema_ident: NameIdentifier = NameIdentifier.of(
        catalog_name, schema_name
    )
    table_ident: NameIdentifier = NameIdentifier.of(
        schema_name, table_name
    )
    table_new_ident: NameIdentifier = NameIdentifier.of(
        schema_name, table_new_name
    )

    column1_name: str = "test_column1"
    column1_data_type: Type = Types.IntegerType(signed=True)
    column1_comment: str = "test_column_comment1"
    column1_nullable: bool = True
    column1_auto_increment: bool = False
    column1_default_value: Expression = {
        "type": "literal",
        "dataType": Types.IntegerType(signed=True).simple_string(),
        "value": "1",
    }

    column2_name: str = "test_column2"
    column2_data_type: Type = Types.VarCharType(length=255)
    column2_comment: str = "test_column_comment2"
    column2_nullable: bool = False
    column2_auto_increment: bool = False
    column2_default_value: Expression = {
        "type": "literal",
        "dataType": Types.VarCharType(length=255).simple_string(),
        "value": "0",
    }

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    gravitino_client: GravitinoClient = None

    def setUp(self):
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        catalog = self.gravitino_client.create_catalog(
            name=self.catalog_name,
            catalog_type=Catalog.Type.RELATIONAL,
            provider=self.catalog_provider,
            comment="",
            properties=self.catalog_properties,
        )
        catalog.as_schemas().create_schema(
            schema_name=self.schema_name,
        )

    def clean_test_data(self):
        try:
            self.gravitino_client = GravitinoClient(
                uri="http://localhost:8090", metalake_name=self.metalake_name
            )
            catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
            logger.info(
                "Drop table %s[%s]",
                self.table_ident,
                catalog.as_table_catalog().drop_table(ident=self.table_ident),
            )
            logger.info(
                "Drop schema %s[%s]",
                self.schema_ident,
                catalog.as_schemas().drop_schema(self.schema_name, cascade=True),
            )
            logger.info(
                "Drop catalog %s[%s]",
                self.catalog_ident,
                self.gravitino_client.drop_catalog(name=self.catalog_name),
            )
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_name,
                self.gravitino_admin_client.drop_metalake(self.metalake_name),
            )
        except Exception as e:
            logger.error("Clean test data failed: %s", e)

    def create_column(
        self,
        name: str,
        data_type: Type,
        comment: str = None,
        nullable: bool = True,
        auto_increment: bool = False,
        default_value: Expression = Expression.EMPTY_EXPRESSION,
    ) -> ColumnDTO:
        return (
            ColumnDTO.builder()
            .with_name(name)
            .with_data_type(data_type)
            .with_comment(comment)
            .with_nullable(nullable)
            .with_auto_increment(auto_increment)
            .with_default_value(default_value)
            .build()
        )

    def create_columns(self) -> List[ColumnDTO]:
        return [
            self.create_column(
                name=self.column1_name,
                data_type=self.column1_data_type,
                comment=self.column1_comment,
                nullable=self.column1_nullable,
                auto_increment=self.column1_auto_increment,
                default_value=self.column1_default_value,
            ),
            self.create_column(
                name=self.column2_name,
                data_type=self.column2_data_type,
                comment=self.column2_comment,
                nullable=self.column2_nullable,
                auto_increment=self.column2_auto_increment,
                default_value=self.column2_default_value,
            ),
        ]

    def create_table(self) -> Table:
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        columns = self.create_columns()

        return catalog.as_table_catalog().create_table(
            ident=self.table_ident,
            columns=columns,
            comment=self.table_comment,
            properties=self.table_properties,
        )

    def test_create_table(self):
        table = self.create_table()
        self.assertEqual(table.name(), self.table_name)
        self.assertEqual(table.comment(), self.table_comment)
        self.assertEqual(table.properties(), self.table_properties)
        self.assertEqual(table.audit_info().creator(), "anonymous")

