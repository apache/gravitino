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
package org.apache.gravitino.policy.sql;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Conformance tests for typed SQL read-restriction definitions. */
class TestRestrictedSqlExpressionParser {
  @ParameterizedTest
  @MethodSource("validFilters")
  void testValidFilters(String source) {
    Assertions.assertDoesNotThrow(
        () -> RestrictedSqlExpressionParserFacade.validateRowFilter(source));
  }

  private static Stream<String> validFilters() {
    return Stream.of(
        "() RETURNS BOOLEAN -> TRUE",
        "() RETURNS BOOLEAN -> NULL",
        "() returns boolean -> current_user() = 'O''Brien'",
        "() RETURNS BOOLEAN -> is_group_member('auditors')",
        "() RETURNS BOOLEAN -> NOT NULL OR FALSE AND TRUE",
        "() RETURNS BOOLEAN -> 1.0 = 1",
        "() RETURNS BOOLEAN -> NULL = NULL",
        "(active BOOLEAN) RETURNS BOOLEAN -> active",
        "(x STRING) RETURNS BOOLEAN -> x <> 'US'",
        "(x STRING) RETURNS BOOLEAN -> NOT (x = 'US')",
        "(x STRING) RETURNS BOOLEAN -> x IS NULL OR x != 'US'",
        "(x STRING) RETURNS BOOLEAN -> x LIKE 'Acme%'",
        "(x STRING) RETURNS BOOLEAN -> x NOT LIKE 'Acme%'",
        "(x STRING) RETURNS BOOLEAN -> x IN ('US', 'CA', NULL)",
        "(x STRING) RETURNS BOOLEAN -> x NOT IN (NULL)",
        "(x STRING) RETURNS BOOLEAN -> x = user_attribute('region')",
        "(x STRING) RETURNS BOOLEAN -> user_attribute('region') IS NULL",
        "(x STRING) RETURNS BOOLEAN -> CASE WHEN x IS NULL THEN FALSE ELSE x = 'US' END",
        "(x STRING) RETURNS BOOLEAN -> CASE x WHEN 'US' THEN TRUE WHEN 'CA' THEN TRUE ELSE FALSE END",
        "(x STRING) RETURNS BOOLEAN -> CASE x WHEN NULL THEN TRUE END",
        "(x STRING) RETURNS BOOLEAN -> CASE WHEN NULL THEN TRUE ELSE CASE x WHEN 'US' THEN TRUE ELSE FALSE END END",
        "(x STRING, y STRING) RETURNS BOOLEAN -> x = y",
        "(x INTEGER) RETURNS BOOLEAN -> x BETWEEN -2147483648 AND 2147483647",
        "(x LONG) RETURNS BOOLEAN -> x NOT BETWEEN -9223372036854775808 AND 9223372036854775807",
        "(x DECIMAL(4,2)) RETURNS BOOLEAN -> x IN (99.99, -99.99, 1.000, NULL)",
        "(x FLOAT) RETURNS BOOLEAN -> is_nan(x) OR x = -0.0",
        "(x DOUBLE) RETURNS BOOLEAN -> NOT is_nan(x)",
        "(x DATE) RETURNS BOOLEAN -> x >= DATE '2024-02-29' AND year(x) = 2024",
        "(x DATE) RETURNS BOOLEAN -> month(x) = 3 OR day(x) = 15",
        "(x TIME(6)) RETURNS BOOLEAN -> x = TIME '12:30:00.123456'",
        "(x TIMESTAMP) RETURNS BOOLEAN -> x = TIMESTAMP '2026-01-01 12:30:00'",
        "(x TIMESTAMP WITHOUT TIME ZONE) RETURNS BOOLEAN -> x = TIMESTAMP '2026-01-01 12:30:00'",
        "(x TIMESTAMP(6) WITH TIME ZONE) RETURNS BOOLEAN -> x = TIMESTAMP WITH TIME ZONE '2026-01-01 12:30:00+00:00'",
        "(val STRING, \"val\" STRING) RETURNS BOOLEAN -> VAL = \"val\"",
        "(\"CASE\" STRING, \"Customer Region\" STRING) RETURNS BOOLEAN -> \"CASE\" = \"Customer Region\"",
        "(\"a\"\"b\" STRING) RETURNS BOOLEAN -> \"a\"\"b\" = '\\n中😀'");
  }

  @ParameterizedTest
  @MethodSource("validMasks")
  void testValidMasks(String source) {
    Assertions.assertDoesNotThrow(
        () -> RestrictedSqlExpressionParserFacade.validateColumnMask(source));
  }

  private static Stream<String> validMasks() {
    Stream<String> strings =
        Stream.of(
                "value",
                "NULL",
                "'XXXXXXXX'",
                "mask_alphanum(value)",
                "show_first_4(value)",
                "show_last_4(value)",
                "mask_to_fixed_value(value)",
                "replace_with_null(value)",
                "sha256_global(value)",
                "sha256_query_local(value)",
                "show_last_4(mask_alphanum(value))",
                "CASE WHEN is_group_member('auditors') THEN show_last_4(value) ELSE NULL END",
                "CASE WHEN value IS NULL THEN NULL ELSE 'XXXXXXXX' END",
                "CASE CURRENT_USER() WHEN 'alice' THEN value END")
            .map(body -> "(value STRING) RETURNS STRING -> " + body);
    return Stream.concat(
        strings,
        Stream.of(
            "(xxx STRING) RETURNS STRING -> show_last_4(xxx)",
            "(value INTEGER) RETURNS INTEGER -> sha256_global(value)",
            "(value LONG) RETURNS LONG -> sha256_query_local(value)",
            "(value BINARY) RETURNS BINARY -> sha256_global(value)",
            "(value BINARY) RETURNS BINARY -> replace_with_null(value)",
            "(value DATE) RETURNS DATE -> truncate_to_year(value)",
            "(value TIMESTAMP WITH TIME ZONE) RETURNS TIMESTAMP(6) WITH TIME ZONE -> truncate_to_month(value)",
            "(value TIME) RETURNS TIME(6) -> value",
            "(value BOOLEAN) RETURNS BOOLEAN -> mask_to_fixed_value(value)",
            "(value DOUBLE) RETURNS DOUBLE -> mask_to_fixed_value(value)",
            "(value DECIMAL(3,1)) RETURNS DECIMAL(3,1) -> CASE WHEN TRUE THEN -99.9 ELSE 0 END"));
  }

  @ParameterizedTest
  @MethodSource("invalidFilters")
  void testInvalidFilters(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedSqlExpressionParserFacade.validateRowFilter(source),
        source);
  }

  private static Stream<String> invalidFilters() {
    Stream<String> bodies =
        Stream.of(
            "'not boolean'",
            "1",
            "unknown",
            "col('x') = 'a'",
            "session_user() = 'a'",
            "TRUE;",
            "TRUE -- comment",
            "TRUE /* comment */",
            "TRUE # comment",
            "SELECT TRUE",
            "TRUE TRUE",
            "1 < 2 < 3",
            "1 == 1",
            "1 '=' 1",
            "TRUE OR 1",
            "NOT 1",
            "NULL IS TRUE",
            "TRUE < FALSE",
            "'1' = 1",
            "TRUE = 1",
            "1 = +1",
            "1 = 01",
            "1 = 1e2",
            "1 = 1.",
            "1 = 1and TRUE",
            "TRUEFALSE",
            "NOTTRUE",
            "CURRENT_USER('a') = 'a'",
            "unknown_function()",
            "is_group_member(CURRENT_USER())",
            "is_group_member('')",
            "user_attribute('') IS NULL",
            "is_nan('a')",
            "year('2026-01-01') = 2026",
            "CASE WHEN 1 THEN TRUE END",
            "CASE WHEN TRUE THEN FALSE ELSE 1 END",
            "CASE WHEN TRUE THEN TRUE ELSE unknown_function() END",
            "CASE TRUE WHEN 'a' THEN TRUE END",
            "CASE WHEN TRUE THEN TRUE",
            "CASE END",
            "CASE WHEN TRUE THEN TRUE ELSE FALSE END END",
            "TRUE IN ()",
            "TRUE IN (TRUE, 1)",
            "TRUE IN [TRUE]",
            "TRUE IN (CURRENT_USER())",
            "TRUE BETWEEN FALSE AND TRUE",
            "'x' LIKE '%Acme'",
            "'x' LIKE '%'",
            "'x' LIKE 'A_me%'",
            "'x' LIKE 'A%%'",
            "'x' LIKE 'Acme%' ESCAPE '!'",
            "TRUE NOT NULL",
            "DATE '2025-02-29' IS NULL",
            "TIME '24:00:00' IS NULL",
            "TIME '12:00:00.1234567' IS NULL",
            "TIMESTAMP WITH TIME ZONE '2026-01-01 12:00:00' IS NULL",
            "TIMESTAMP '2026-01-01 12:00:00Z' IS NULL",
            "user_attribute('k') = 's' AND user_attribute('k') = TRUE");
    return Stream.concat(
        bodies.map(body -> "() RETURNS BOOLEAN -> " + body),
        Stream.of(
            "",
            " ",
            "TRUE",
            "filter := true",
            "(x STRING) -> TRUE",
            "RETURNS BOOLEAN -> TRUE",
            "() RETURNS STRING -> TRUE",
            "(x INTEGER, X INTEGER) RETURNS BOOLEAN -> TRUE",
            "(\"X\" STRING, x STRING) RETURNS BOOLEAN -> TRUE",
            "(TRUE STRING) RETURNS BOOLEAN -> TRUE",
            "(x INT) RETURNS BOOLEAN -> TRUE",
            "(x ARRAY) RETURNS BOOLEAN -> TRUE",
            "(x DECIMAL(0,0)) RETURNS BOOLEAN -> TRUE",
            "(x DECIMAL(39,0)) RETURNS BOOLEAN -> TRUE",
            "(x DECIMAL(2,3)) RETURNS BOOLEAN -> TRUE",
            "(x DECIMAL(-1,0)) RETURNS BOOLEAN -> TRUE",
            "(x TIME(3)) RETURNS BOOLEAN -> TRUE",
            "(x TIMESTAMP(9)) RETURNS BOOLEAN -> TRUE",
            "(x INTEGER) RETURNS BOOLEAN -> x = 2147483648",
            "(x LONG) RETURNS BOOLEAN -> x = -9223372036854775809",
            "(x INTEGER) RETURNS BOOLEAN -> x = 1.1",
            "(x DECIMAL(3,2)) RETURNS BOOLEAN -> x = 10.00",
            "(x DECIMAL(3,2)) RETURNS BOOLEAN -> x = 1.001",
            "(x STRING) RETURNS BOOLEAN -> x = 1",
            "(x INTEGER, y LONG) RETURNS BOOLEAN -> x = y",
            "(x BINARY) RETURNS BOOLEAN -> x = x",
            "(x STRING) RETURNS BOOLEAN -> x IN ('a', x)",
            "(x STRING) RETURNS BOOLEAN -> x = \"a\"",
            "(x STRING) RETURNS BOOLEAN -> x.field = 'a'",
            "(x STRING) RETURNS BOOLEAN -> x = 'unterminated",
            "(\"\" STRING) RETURNS BOOLEAN -> TRUE"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "() RETURNS STRING -> 'x'",
        "(a STRING, b STRING) RETURNS STRING -> a",
        "(value STRING) RETURNS INTEGER -> 1",
        "(value STRING) RETURNS STRING -> phone_number",
        "(value STRING) RETURNS STRING -> CASE WHEN TRUE THEN value ELSE 1 END",
        "(value STRING) RETURNS STRING -> show_last_4(value, 4)",
        "(value INTEGER) RETURNS INTEGER -> show_last_4(value)",
        "(value DOUBLE) RETURNS DOUBLE -> sha256_global(value)",
        "(value TIME) RETURNS TIME -> mask_to_fixed_value(value)",
        "(value STRING) RETURNS STRING -> truncate_to_month(value)",
        "(value STRING) RETURNS STRING -> unknown(value)",
        "(value DECIMAL(3,1)) RETURNS DECIMAL(3,2) -> NULL",
        "(value TIMESTAMP) RETURNS TIMESTAMP WITH TIME ZONE -> NULL",
        "mask := action('show-last-4')"
      })
  void testInvalidMasks(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedSqlExpressionParserFacade.validateColumnMask(source));
  }

  @Test
  void testPreservesCaseNullAndPredicateStructure() {
    SqlDefinition definition =
        RestrictedSqlExpressionParserFacade.parse(
            "(x STRING) RETURNS BOOLEAN -> CASE WHEN x = 'US' THEN TRUE WHEN x IS NULL THEN FALSE END",
            false);
    Assertions.assertEquals("CASE", definition.body.operator);
    Assertions.assertEquals(5, definition.body.children.size());
    Assertions.assertEquals("=", definition.body.children.get(0).operator);
    Assertions.assertEquals("IS NULL", definition.body.children.get(2).operator);
    Assertions.assertEquals("NULL", definition.body.children.get(4).operator);
    SqlDefinition negation =
        RestrictedSqlExpressionParserFacade.parse(
            "(x STRING) RETURNS BOOLEAN -> NOT x = 'US' OR x IS NULL AND FALSE", false);
    Assertions.assertEquals("OR", negation.body.operator);
    Assertions.assertEquals("NOT", negation.body.children.get(0).operator);
    Assertions.assertEquals("AND", negation.body.children.get(1).operator);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> definition.parameters.clear());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> definition.body.children.clear());
  }

  @Test
  void testIdentifierAndLiteralPreservation() {
    SqlDefinition definition =
        RestrictedSqlExpressionParserFacade.parse(
            "(\"a\"\"b\" STRING) RETURNS BOOLEAN -> \"a\"\"b\" = 'O''Brien\\n😀'", false);
    Assertions.assertTrue(definition.parameters.containsKey("a\"b"));
    Assertions.assertEquals("O'Brien\\n😀", definition.body.children.get(1).value);
    SqlDefinition mask =
        RestrictedSqlExpressionParserFacade.parse(
            "(local_name STRING) RETURNS STRING -> show_last_4(local_name)", true);
    Assertions.assertEquals("LOCAL_NAME", mask.body.children.get(0).value);
    SqlDefinition zero =
        RestrictedSqlExpressionParserFacade.parse("(x DOUBLE) RETURNS DOUBLE -> -0.0", true);
    Assertions.assertEquals("-0.0", zero.body.value);
  }

  @Test
  void testSourceAndLiteralLimits() {
    String prefix = "() RETURNS BOOLEAN -> TRUE";
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedSqlExpressionParserFacade.validateRowFilter(
                prefix + " ".repeat(16384 - prefix.length())));
    reject(prefix + " ".repeat(16385 - prefix.length()));
    filter("'" + "中".repeat(1365) + "a' IS NULL");
    rejectBody("'" + "中".repeat(1365) + "aa' IS NULL");
    filter("1 = " + "9".repeat(256));
    rejectBody("1 = " + "9".repeat(257));
    rejectBody("'\uD800' IS NULL");
    rejectBody("'\uDC00' IS NULL");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedSqlExpressionParserFacade.validateRowFilter(null));
    reject("(x FLOAT) RETURNS BOOLEAN -> x = " + "9".repeat(40));
  }

  @Test
  void testDepthNodeAndListLimits() {
    filter("NOT ".repeat(8) + "TRUE");
    rejectBody("NOT ".repeat(9) + "TRUE");
    rejectBody("NOT ".repeat(2000) + "TRUE");
    filter("(".repeat(256) + "TRUE" + ")".repeat(256));
    rejectBody("(".repeat(257) + "TRUE" + ")".repeat(257));
    filter(String.join(" OR ", Collections.nCopies(255, "TRUE")));
    rejectBody(String.join(" OR ", Collections.nCopies(256, "TRUE")));
    // A list has a predicate node, a left operand, and one node per literal.
    filter("1 IN (" + numbers(254) + ")");
    rejectBody("1 IN (" + numbers(255) + ")");
    rejectBody("1 IN (" + numbers(257) + ")");
    rejectBody("CASE WHEN TRUE THEN ".repeat(9) + "TRUE" + " END".repeat(9));
    rejectBody("show_last_4(".repeat(9) + "'x'" + ")".repeat(9) + " IS NULL");
  }

  @Test
  void testSyntaxErrorsHaveLocation() {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedSqlExpressionParserFacade.validateRowFilter(
                    "() RETURNS BOOLEAN ->\n TRUE;"));
    Assertions.assertTrue(error.getMessage().contains("line 2, column"));
  }

  private static String numbers(int count) {
    return IntStream.range(0, count).mapToObj(Integer::toString).collect(Collectors.joining(","));
  }

  private static void filter(String body) {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedSqlExpressionParserFacade.validateRowFilter("() RETURNS BOOLEAN -> " + body));
  }

  private static void rejectBody(String body) {
    reject("() RETURNS BOOLEAN -> " + body);
  }

  private static void reject(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedSqlExpressionParserFacade.validateRowFilter(source));
  }
}
