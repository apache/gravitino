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

from random import randint

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, NameIdentifier
from gravitino.api.function.function_change import FunctionChange
from gravitino.api.function.function_definition import FunctionDefinitions
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_param import FunctionParams
from gravitino.api.function.function_type import FunctionType
from gravitino.api.function.sql_impl import SQLImpl
from gravitino.api.rel.types.types import Types
from gravitino.exceptions.base import (
    FunctionAlreadyExistsException,
    NoSuchFunctionException,
)
from gravitino.namespace import Namespace
from tests.integration.integration_test_env import IntegrationTestEnv


class TestFunctionCatalog(IntegrationTestEnv):
    """Integration tests for Function catalog operations."""

    _metalake_name: str = "function_it_metalake" + str(randint(0, 1000))
    _catalog_name: str = "function_it_catalog" + str(randint(0, 1000))
    _schema_name: str = "function_it_schema" + str(randint(0, 1000))

    _gravitino_admin_client: GravitinoAdminClient = None
    _gravitino_client: GravitinoClient = None
    _catalog: Catalog = None

    @classmethod
    def setUpClass(cls):
        """Set up the integration test environment."""
        super().setUpClass()

        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
        cls._gravitino_admin_client.create_metalake(
            cls._metalake_name, comment="comment", properties={}
        )

        cls._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls._metalake_name
        )
        cls._catalog = cls._gravitino_client.create_catalog(
            name=cls._catalog_name,
            catalog_type=Catalog.Type.FILESET,
            provider="hadoop",
            comment="comment",
            properties={},
        )

    @classmethod
    def tearDownClass(cls):
        """Tear down the integration test environment."""
        cls._gravitino_client.drop_catalog(name=cls._catalog_name, force=True)
        cls._gravitino_admin_client.drop_metalake(name=cls._metalake_name, force=True)

        super().tearDownClass()

    def setUp(self):
        """Set up schema before each test."""
        self._catalog.as_schemas().create_schema(self._schema_name, "comment", {})

    def tearDown(self):
        """Drop schema after each test."""
        self._catalog.as_schemas().drop_schema(self._schema_name, True)

    def _create_spark_impl(self, sql: str = "SELECT 1") -> SQLImpl:
        """Create a Spark SQL implementation for testing."""
        return (
            SQLImpl.builder()
            .with_runtime_type(SQLImpl.RuntimeType.SPARK)
            .with_sql(sql)
            .build()
        )

    def _create_trino_impl(self, sql: str = "SELECT 1") -> SQLImpl:
        """Create a Trino SQL implementation for testing."""
        return (
            SQLImpl.builder()
            .with_runtime_type(SQLImpl.RuntimeType.TRINO)
            .with_sql(sql)
            .build()
        )

    def _create_scalar_definition(self):
        """Create a scalar function definition with no parameters for testing."""
        return FunctionDefinitions.of(
            [], Types.IntegerType.get(), [self._create_spark_impl()]
        )

    def test_create_get_function(self):
        """Test creating and retrieving a function."""
        function_name = "function_it_function" + str(randint(0, 1000))
        function_ident = NameIdentifier.of(self._schema_name, function_name)
        comment = "comment"
        definition = self._create_scalar_definition()

        # Test create function
        function = self._catalog.as_function_catalog().register_function(
            ident=function_ident,
            comment=comment,
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[definition],
        )
        self.assertEqual(function_name, function.name())
        self.assertEqual(comment, function.comment())
        self.assertEqual(FunctionType.SCALAR, function.function_type())
        self.assertTrue(function.deterministic())

        # Test get function
        function = self._catalog.as_function_catalog().get_function(function_ident)
        self.assertEqual(function_name, function.name())
        self.assertEqual(comment, function.comment())
        self.assertEqual(FunctionType.SCALAR, function.function_type())
        self.assertEqual(
            Types.IntegerType.get(), function.definitions()[0].return_type()
        )

        # Test create exists function
        with self.assertRaises(FunctionAlreadyExistsException):
            self._catalog.as_function_catalog().register_function(
                ident=function_ident,
                comment=comment,
                function_type=FunctionType.SCALAR,
                deterministic=True,
                definitions=[definition],
            )

    def test_list_functions(self):
        """Test listing functions in a schema."""
        function_name1 = "function_it_function1" + str(randint(0, 1000))
        function_name2 = "function_it_function2" + str(randint(0, 1000))
        function_ident1 = NameIdentifier.of(self._schema_name, function_name1)
        function_ident2 = NameIdentifier.of(self._schema_name, function_name2)
        definition = self._create_scalar_definition()

        self._catalog.as_function_catalog().register_function(
            ident=function_ident1,
            comment="comment",
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[definition],
        )
        self._catalog.as_function_catalog().register_function(
            ident=function_ident2,
            comment="comment",
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[definition],
        )

        idents = self._catalog.as_function_catalog().list_functions(
            Namespace.of(self._schema_name)
        )
        self.assertEqual(2, len(idents))
        self.assertTrue(function_ident1 in idents)
        self.assertTrue(function_ident2 in idents)

        functions = self._catalog.as_function_catalog().list_function_infos(
            Namespace.of(self._schema_name)
        )
        self.assertEqual(2, len(functions))
        self.assertEqual(function_name1, functions[0].name())
        self.assertEqual(function_name2, functions[1].name())

    def test_alter_function(self):
        """Test altering a function with all supported FunctionChange types."""
        function_name = "function_it_function" + str(randint(0, 1000))
        function_ident = NameIdentifier.of(self._schema_name, function_name)
        definition = self._create_scalar_definition()

        self._catalog.as_function_catalog().register_function(
            ident=function_ident,
            comment="comment",
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[definition],
        )

        # Test update comment
        function = self._catalog.as_function_catalog().alter_function(
            function_ident, FunctionChange.update_comment("new comment")
        )
        self.assertEqual("new comment", function.comment())
        function = self._catalog.as_function_catalog().get_function(function_ident)
        self.assertEqual("new comment", function.comment())

        # Test add_definition: add an overload with one parameter
        x_param = FunctionParams.of("x", Types.IntegerType.get())
        new_definition = FunctionDefinitions.of(
            [x_param], Types.IntegerType.get(), [self._create_spark_impl("SELECT x")]
        )
        function = self._catalog.as_function_catalog().alter_function(
            function_ident, FunctionChange.add_definition(new_definition)
        )
        self.assertEqual(2, len(function.definitions()))

        function = self._catalog.as_function_catalog().get_function(function_ident)
        self.assertEqual(2, len(function.definitions()))

        # Test add_impl: add a Trino implementation to the no-arg definition
        trino_impl = self._create_trino_impl()
        function = self._catalog.as_function_catalog().alter_function(
            function_ident,
            FunctionChange.add_impl([], trino_impl),
        )
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        impl_runtimes = [i.runtime() for i in no_arg_def.impls()]
        self.assertIn(FunctionImpl.RuntimeType.TRINO, impl_runtimes)

        function = self._catalog.as_function_catalog().get_function(function_ident)
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        impl_runtimes = [i.runtime() for i in no_arg_def.impls()]
        self.assertIn(FunctionImpl.RuntimeType.TRINO, impl_runtimes)

        # Test update_impl: replace the Trino implementation with new SQL
        updated_trino_impl = self._create_trino_impl("SELECT 2")
        function = self._catalog.as_function_catalog().alter_function(
            function_ident,
            FunctionChange.update_impl([], FunctionImpl.RuntimeType.TRINO, updated_trino_impl),
        )
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        trino_impls = [i for i in no_arg_def.impls() if i.runtime() == FunctionImpl.RuntimeType.TRINO]
        self.assertEqual(1, len(trino_impls))
        self.assertEqual("SELECT 2", trino_impls[0].sql())

        function = self._catalog.as_function_catalog().get_function(function_ident)
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        trino_impls = [i for i in no_arg_def.impls() if i.runtime() == FunctionImpl.RuntimeType.TRINO]
        self.assertEqual("SELECT 2", trino_impls[0].sql())

        # Test remove_impl: remove the Trino implementation from the no-arg definition
        function = self._catalog.as_function_catalog().alter_function(
            function_ident,
            FunctionChange.remove_impl([], FunctionImpl.RuntimeType.TRINO),
        )
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        impl_runtimes = [i.runtime() for i in no_arg_def.impls()]
        self.assertNotIn(FunctionImpl.RuntimeType.TRINO, impl_runtimes)

        function = self._catalog.as_function_catalog().get_function(function_ident)
        no_arg_def = next(d for d in function.definitions() if not d.parameters())
        impl_runtimes = [i.runtime() for i in no_arg_def.impls()]
        self.assertNotIn(FunctionImpl.RuntimeType.TRINO, impl_runtimes)

        # Test remove_definition: remove the parameterized overload
        function = self._catalog.as_function_catalog().alter_function(
            function_ident, FunctionChange.remove_definition([x_param])
        )
        self.assertEqual(1, len(function.definitions()))

        function = self._catalog.as_function_catalog().get_function(function_ident)
        self.assertEqual(1, len(function.definitions()))

    def test_drop_function(self):
        """Test dropping a function."""
        function_name = "function_it_function" + str(randint(0, 1000))
        function_ident = NameIdentifier.of(self._schema_name, function_name)
        definition = self._create_scalar_definition()

        self._catalog.as_function_catalog().register_function(
            ident=function_ident,
            comment="comment",
            function_type=FunctionType.SCALAR,
            deterministic=True,
            definitions=[definition],
        )

        self.assertTrue(
            self._catalog.as_function_catalog().drop_function(function_ident)
        )
        self.assertFalse(
            self._catalog.as_function_catalog().drop_function(function_ident)
        )

        with self.assertRaises(NoSuchFunctionException):
            self._catalog.as_function_catalog().get_function(function_ident)
