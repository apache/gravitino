/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.spark.connector.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestBuiltinFunctionSupport {

  private final BuiltinFunctionSupport builtinFunctionSupport = new BuiltinFunctionSupport();
  private static final String DEFAULT_NAMESPACE = "default";

  @Test
  void testListFunctionsInDefaultNamespace() throws NoSuchNamespaceException {
    Identifier[] functions = builtinFunctionSupport.listFunctions(new String[0], DEFAULT_NAMESPACE);
    Assertions.assertEquals(1, functions.length);
    Assertions.assertEquals(MyStringLengthFunction.NAME, functions[0].name());

    Identifier[] functionsInDefault =
        builtinFunctionSupport.listFunctions(new String[] {DEFAULT_NAMESPACE}, DEFAULT_NAMESPACE);
    Assertions.assertEquals(1, functionsInDefault.length);
    Assertions.assertEquals(MyStringLengthFunction.NAME, functionsInDefault[0].name());
  }

  @Test
  void testListFunctionsInUnsupportedNamespace() {
    Assertions.assertThrows(
        NoSuchNamespaceException.class,
        () -> builtinFunctionSupport.listFunctions(new String[] {"other"}, DEFAULT_NAMESPACE));
  }

  @Test
  void testLoadFunctionAndExecute() throws NoSuchFunctionException {
    UnboundFunction unboundFunction =
        builtinFunctionSupport.loadFunction(
            Identifier.of(new String[0], MyStringLengthFunction.NAME), DEFAULT_NAMESPACE);
    Assertions.assertInstanceOf(MyStringLengthFunction.class, unboundFunction);

    StructType inputSchema =
        new StructType(
            new StructField[] {
              new StructField("col", DataTypes.StringType, true, Metadata.empty())
            });
    BoundFunction boundFunction = unboundFunction.bind(inputSchema);
    ScalarFunction<?> scalarFunction = (ScalarFunction<?>) boundFunction;

    InternalRow row = new GenericInternalRow(new Object[] {UTF8String.fromString("abcde")});
    Assertions.assertEquals(5, scalarFunction.produceResult(row));

    InternalRow nullRow = new GenericInternalRow(1);
    nullRow.setNullAt(0);
    Assertions.assertNull(scalarFunction.produceResult(nullRow));
  }

  @Test
  void testLoadFunctionWithWrongNamespaceOrName() {
    Assertions.assertThrows(
        NoSuchFunctionException.class,
        () ->
            builtinFunctionSupport.loadFunction(
                Identifier.of(new String[] {"other"}, MyStringLengthFunction.NAME),
                DEFAULT_NAMESPACE));
    Assertions.assertThrows(
        NoSuchFunctionException.class,
        () ->
            builtinFunctionSupport.loadFunction(
                Identifier.of(new String[0], "unknown_func"), DEFAULT_NAMESPACE));
  }

  @Test
  void testBindValidatesInputType() throws NoSuchFunctionException {
    UnboundFunction unboundFunction =
        builtinFunctionSupport.loadFunction(
            Identifier.of(new String[0], MyStringLengthFunction.NAME), DEFAULT_NAMESPACE);
    StructType invalidSchema =
        new StructType(
            new StructField[] {
              new StructField("col", DataTypes.IntegerType, true, Metadata.empty())
            });
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> unboundFunction.bind(invalidSchema));
  }
}
