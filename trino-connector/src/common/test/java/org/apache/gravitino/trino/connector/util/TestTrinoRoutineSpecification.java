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
package org.apache.gravitino.trino.connector.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestTrinoRoutineSpecification {

  private static final GeneralDataTypeTransformer TRANSFORMER = new GeneralDataTypeTransformer();
  private static final SqlParser PARSER = new SqlParser();

  @Test
  public void testBareExpression() {
    String spec = build("fn_double", "n * 2", param("n", Types.IntegerType.get()));
    assertEquals(
        "FUNCTION \"fn_double\"(\"n\" integer) RETURNS integer DETERMINISTIC SECURITY INVOKER"
            + " RETURN n * 2",
        spec);
    parse(spec);
  }

  @Test
  public void testReturnStatementIsNotWrapped() {
    for (String body :
        new String[] {
          "RETURN x + 1",
          "return x + 1",
          "RETURN(x + 1)",
          "RETURN/* comment */ x + 1",
          "/* comment */ RETURN x + 1",
          "-- comment\nRETURN x + 1",
          "  /* a */ -- b\n /* c */ RETURN x + 1"
        }) {
      String spec = build("f", body, param("x", Types.IntegerType.get()));
      parse(spec);
      assertTrue(
          spec.endsWith(
              " SECURITY INVOKER " + TrinoRoutineSpecification.stripLeadingComments(body)),
          spec);
    }
  }

  @Test
  public void testBeginBlockIsNotWrapped() {
    String spec = build("f", "BEGIN RETURN x; END", param("x", Types.IntegerType.get()));
    assertEquals(
        "FUNCTION \"f\"(\"x\" integer) RETURNS integer DETERMINISTIC SECURITY INVOKER"
            + " BEGIN RETURN x; END",
        spec);
    parse(spec);
  }

  @Test
  public void testIdentifierLikeReturnPrefixIsAnExpression() {
    String spec = build("f", "returned + 1", param("returned", Types.IntegerType.get()));
    assertEquals(
        "FUNCTION \"f\"(\"returned\" integer) RETURNS integer DETERMINISTIC SECURITY INVOKER"
            + " RETURN returned + 1",
        spec);
    parse(spec);
  }

  @Test
  public void testParameterNamedLikeKeywordShadowsTheKeyword() {
    for (String name : new String[] {"return", "begin", "function", "RETURN"}) {
      String spec = build("f", name + " + 1", param(name, Types.IntegerType.get()));
      parse(spec);
      assertTrue(spec.endsWith(" SECURITY INVOKER RETURN " + name + " + 1"), spec);
      assertEquals("RETURN (" + name + " + 1)", formatStatement(spec), spec);
    }
  }

  @Test
  public void testReservedWordsAndSpecialNamesAreQuoted() {
    String spec =
        build(
            "order",
            "1",
            param("select", Types.IntegerType.get()),
            param("value-with-dash", Types.IntegerType.get()),
            param("say \"hi\"", Types.IntegerType.get()));
    assertEquals(
        "FUNCTION \"order\"(\"select\" integer, \"value-with-dash\" integer,"
            + " \"say \"\"hi\"\"\" integer) RETURNS integer DETERMINISTIC SECURITY INVOKER"
            + " RETURN 1",
        spec);
    parse(spec);
  }

  @Test
  public void testNestedRowFieldNamesAreQuoted() {
    Type row =
        Types.StructType.of(
            Types.StructType.Field.nullableField("value-with-dash", Types.IntegerType.get()),
            Types.StructType.Field.nullableField("select", Types.StringType.get()));
    Type nested = Types.MapType.valueNullable(Types.StringType.get(), Types.ListType.nullable(row));
    String spec = build("f", "x", Types.ListType.nullable(row), param("x", nested));
    assertEquals(
        "FUNCTION \"f\"(\"x\" map(varchar,array(row(\"value-with-dash\" integer,\"select\""
            + " varchar)))) RETURNS array(row(\"value-with-dash\" integer,\"select\" varchar))"
            + " DETERMINISTIC SECURITY INVOKER RETURN x",
        spec);
    parse(spec);
  }

  @Test
  public void testNonDeterministic() {
    String spec =
        TrinoRoutineSpecification.build(
            function("f", false), definition(Types.IntegerType.get()), "random()", TRANSFORMER);
    assertEquals(
        "FUNCTION \"f\"() RETURNS integer NOT DETERMINISTIC SECURITY INVOKER RETURN random()",
        spec);
    parse(spec);
  }

  @Test
  public void testFullSpecificationIsRejected() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                build(
                    "f",
                    "/* c */ FUNCTION f(x integer) RETURNS integer RETURN x",
                    param("x", Types.IntegerType.get())));
    assertEquals(
        "The SQL body of function f must be an expression or a RETURN/BEGIN statement, not a"
            + " full FUNCTION specification",
        e.getMessage());
  }

  @Test
  public void testMissingReturnTypeIsRejected() {
    TrinoException e =
        assertThrows(
            TrinoException.class,
            () -> build("f", "1", (Type) null, param("x", Types.IntegerType.get())));
    assertEquals("Function f has a definition without a return type", e.getMessage());
  }

  private static void parse(String spec) {
    PARSER.createFunctionSpecification(spec);
  }

  // Renders the routine body as Trino understands it, so a test can assert that a parameter
  // reference survived instead of being swallowed by a keyword.
  private static String formatStatement(String spec) {
    String formatted = SqlFormatter.formatSql(PARSER.createFunctionSpecification(spec));
    return formatted.substring(formatted.indexOf("RETURN ")).trim();
  }

  private static String build(String name, String body, FunctionParam... params) {
    return build(name, body, Types.IntegerType.get(), params);
  }

  private static String build(String name, String body, Type returnType, FunctionParam... params) {
    return TrinoRoutineSpecification.build(
        function(name, true), definition(returnType, params), body, TRANSFORMER);
  }

  private static Function function(String name, boolean deterministic) {
    Function function = mock(Function.class);
    when(function.name()).thenReturn(name);
    when(function.functionType()).thenReturn(FunctionType.SCALAR);
    when(function.deterministic()).thenReturn(deterministic);
    return function;
  }

  private static FunctionDefinition definition(Type returnType, FunctionParam... params) {
    FunctionDefinition definition = mock(FunctionDefinition.class);
    when(definition.parameters()).thenReturn(params);
    when(definition.returnType()).thenReturn(returnType);
    return definition;
  }

  private static FunctionParam param(String name, Type type) {
    FunctionParam param = mock(FunctionParam.class);
    when(param.name()).thenReturn(name);
    when(param.dataType()).thenReturn(type);
    return param;
  }
}
