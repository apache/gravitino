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
package org.apache.gravitino.policy.rego;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.gravitino.policy.rego.CanonicalExpression.Column;
import org.apache.gravitino.policy.rego.CanonicalExpression.Comparison;
import org.apache.gravitino.policy.rego.CanonicalExpression.ComparisonOperator;
import org.apache.gravitino.policy.rego.CanonicalExpression.GroupMembership;
import org.apache.gravitino.policy.rego.CanonicalExpression.Literal;
import org.apache.gravitino.policy.rego.CanonicalExpression.LiteralArray;
import org.apache.gravitino.policy.rego.CanonicalExpression.LiteralType;
import org.apache.gravitino.policy.rego.CanonicalExpression.LogicalExpression;
import org.apache.gravitino.policy.rego.CanonicalExpression.LogicalOperator;
import org.apache.gravitino.policy.rego.CanonicalExpression.Not;
import org.apache.gravitino.policy.rego.CanonicalExpression.SessionUser;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RuleType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link RestrictedRegoExpressionParserFacade}. */
public class TestRestrictedRegoExpressionParserFacade {

  @Test
  void testParsesTypedRules() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"owner\") == session_user()");
    Assertions.assertEquals(RuleType.FILTER, filter.ruleType());
    Assertions.assertFalse(filter.isConditional());
    Assertions.assertInstanceOf(Comparison.class, filter.fallback());

    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask("mask := action(\"show-last-4\")");
    Assertions.assertEquals(RuleType.MASK, mask.ruleType());
    Assertions.assertFalse(mask.isConditional());
    Assertions.assertEquals(MaskAction.SHOW_LAST_4, mask.fallback());

    Assertions.assertInstanceOf(
        RowFilter.class, RestrictedRegoExpressionParserFacade.parse("filter := true"));
    Assertions.assertInstanceOf(
        ColumnMask.class,
        RestrictedRegoExpressionParserFacade.parse("mask := action(\"mask-alphanum\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter("mask := action(\"show-last-4\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parseColumnMask("filter := true"));
  }

  @Test
  void testParsesContextReferences() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"owner\") == session_user()");

    Assertions.assertEquals(ComparisonOperator.EQ, comparison.operator());
    Assertions.assertEquals("owner", ((Column) comparison.left()).name());
    Assertions.assertInstanceOf(SessionUser.class, comparison.right());

    GroupMembership membership =
        (GroupMembership) parseFilterExpression("is_group_member(\"auditors\")");
    Assertions.assertEquals("auditors", membership.group());
  }

  @Test
  void testParsesPrecedenceAndLiteralArray() {
    LogicalExpression or =
        (LogicalExpression)
            parseFilterExpression(
                "col(\"region\") in [\"US\", \"CA\"] or "
                    + "col(\"level\") >= 3 and not col(\"deleted\") == true");

    Assertions.assertEquals(LogicalOperator.OR, or.operator());
    Assertions.assertEquals(2, or.operands().size());

    Comparison membership = (Comparison) or.operands().get(0);
    Assertions.assertEquals(ComparisonOperator.IN, membership.operator());
    LiteralArray values = (LiteralArray) membership.right();
    Assertions.assertEquals(LiteralType.STRING, values.elementType());
    Assertions.assertEquals(
        List.of("US", "CA"),
        List.of(values.values().get(0).value(), values.values().get(1).value()));

    LogicalExpression and = (LogicalExpression) or.operands().get(1);
    Assertions.assertEquals(LogicalOperator.AND, and.operator());
    Assertions.assertEquals(
        ComparisonOperator.GTE, ((Comparison) and.operands().get(0)).operator());
    Not not = (Not) and.operands().get(1);
    Assertions.assertEquals(ComparisonOperator.EQ, ((Comparison) not.operand()).operator());
  }

  @Test
  void testParsesConditionalRowFilterAndLowersFirstMatch() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := col(\"tenant\") == session_user() if is_group_member(\"member\") "
                + "else := col(\"public\") == true if session_user() == \"guest\" "
                + "else := false");

    Assertions.assertTrue(filter.isConditional());
    Assertions.assertEquals(2, filter.branches().size());
    Assertions.assertInstanceOf(GroupMembership.class, filter.branches().get(0).condition());
    Assertions.assertEquals(
        ComparisonOperator.EQ, ((Comparison) filter.branches().get(1).condition()).operator());
    Assertions.assertEquals(false, ((Literal) filter.fallback()).value());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> filter.branches().add(null));

    CanonicalExpression expected =
        parseFilterExpression(
            "(is_group_member(\"member\") and col(\"tenant\") == session_user()) or "
                + "(not is_group_member(\"member\") and session_user() == \"guest\" and "
                + "col(\"public\") == true) or "
                + "(not is_group_member(\"member\") and "
                + "not session_user() == \"guest\" and false)");
    Assertions.assertEquals(expected, filter.lower());
    Assertions.assertEquals(4, filter.lower().depth());
  }

  @Test
  void testParsesConditionalColumnMask() {
    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask(
            "mask := action(\"show-last-4\") if is_group_member(\"support\") "
                + "else := action(\"sha-256-query-local\") if session_user() == \"service\" "
                + "else := action(\"replace-with-null\")");

    Assertions.assertTrue(mask.isConditional());
    Assertions.assertEquals(2, mask.branches().size());
    Assertions.assertEquals(MaskAction.SHOW_LAST_4, mask.branches().get(0).action());
    Assertions.assertEquals(MaskAction.SHA_256_QUERY_LOCAL, mask.branches().get(1).action());
    Assertions.assertEquals(MaskAction.REPLACE_WITH_NULL, mask.fallback());
    Assertions.assertInstanceOf(GroupMembership.class, mask.branches().get(0).condition());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "mask-alphanum",
        "mask-to-fixed-value",
        "replace-with-null",
        "show-first-4",
        "show-last-4",
        "truncate-to-year",
        "truncate-to-month",
        "sha-256-global",
        "sha-256-query-local"
      })
  void testAcceptsAllMaskActions(String action) {
    ColumnMask mask =
        RestrictedRegoExpressionParserFacade.parseColumnMask("mask := action(\"" + action + "\")");
    Assertions.assertEquals(action, mask.fallback().value());
  }

  @Test
  void testRetainsExactNumbersAndNegativeZero() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"ratio\") == -0.00");
    Literal literal = (Literal) comparison.right();

    Assertions.assertEquals(new BigDecimal("0.00"), literal.value());
    Assertions.assertTrue(literal.negativeZero());

    Comparison positiveZero = (Comparison) parseFilterExpression("col(\"ratio\") == 0.00");
    Assertions.assertNotEquals(literal, positiveZero.right());

    Comparison one = (Comparison) parseFilterExpression("col(\"x\") == 1");
    Comparison onePointZero = (Comparison) parseFilterExpression("col(\"x\") == 1.00");
    Assertions.assertEquals(one.right(), onePointZero.right());
  }

  @Test
  void testDecodesJsonStringsWithoutNormalizing() {
    Comparison comparison = (Comparison) parseFilterExpression("col(\"a\\\"b.c\") == \"a'b\\n\"");

    Assertions.assertEquals("a\"b.c", ((Column) comparison.left()).name());
    Assertions.assertEquals("a'b\n", ((Literal) comparison.right()).value());

    String escapedPair = "\\" + "uD83D" + "\\" + "uDE00";
    Comparison unicode =
        (Comparison) parseFilterExpression("col(\"emoji\") == \"" + escapedPair + "\"");
    Assertions.assertEquals("😀", ((Literal) unicode.right()).value());
  }

  @Test
  void testSourceDepthLimit() {
    CanonicalExpression depthEight =
        parseFilterExpression("not not not not not not not col(\"active\") == true");
    Assertions.assertEquals(8, depthEight.depth());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("not not not not not not not not col(\"active\") == true"));
  }

  @Test
  void testFlattensAssociativeBooleanChains() {
    LogicalExpression and =
        (LogicalExpression)
            parseFilterExpression(
                "true and true and true and true and true and true and true and true and true");
    Assertions.assertEquals(LogicalOperator.AND, and.operator());
    Assertions.assertEquals(9, and.operands().size());
    Assertions.assertEquals(2, and.depth());
  }

  @Test
  void testChecksLoweredRowFilterDepthAtSaveTime() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(
                "filter := true if not not not not col(\"active\") == true else := false"));

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := true if not not not not not col(\"active\") == true "
                        + "else := false"));
    Assertions.assertTrue(error.getMessage().contains("lowered row-filter depth"));
  }

  @Test
  void testFlattensLoweredLogicalOperands() {
    RowFilter filter =
        RestrictedRegoExpressionParserFacade.parseRowFilter(
            "filter := true if is_group_member(\"g\") else := "
                + "true and (true or (true and (true or (true and (true or true)))))");

    Assertions.assertEquals(8, filter.lower().depth());
    LogicalExpression lowered = (LogicalExpression) filter.lower();
    LogicalExpression fallback = (LogicalExpression) lowered.operands().get(1);
    Assertions.assertEquals(3, fallback.operands().size());
  }

  @Test
  void testChecksLoweredRowFilterNodeCountAtSaveTime() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(conditionalFilterWithBranches(9)));

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    conditionalFilterWithBranches(10)));
    Assertions.assertTrue(error.getMessage().contains("lowered row-filter AST"));
  }

  @Test
  void testSemanticErrorsIncludeOffendingSubExpression() {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> parseFilterExpression("col(\"b\") < true"));

    Assertions.assertTrue(error.getMessage().contains("boolean literal supports only == and !="));
    Assertions.assertTrue(error.getMessage().contains("offending comparison"));
    Assertions.assertTrue(error.getMessage().contains("name='b'"));
  }

  @Test
  void testRejectsInvalidUnicodeScalarsAndNul() {
    String isolatedSurrogate = "\\" + "uD800";
    String nul = "\\" + "u0000";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"" + isolatedSurrogate + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"x\") == \"" + isolatedSurrogate + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"" + nul + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("is_group_member(\"" + nul + "\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"x\") == \"" + nul + "\""));
  }

  @Test
  void testRejectsNonAsciiUnicodeEscapeDigits() {
    List<String> invalidEscapes =
        List.of(
            unicodeEscape(0x0660, 0x0660, 0x0664, 0x0661),
            unicodeEscape(0xFF10, 0xFF10, 0xFF14, 0xFF11),
            unicodeEscape(0x0966, 0x0966, 0x096A, 0x0967),
            unicodeEscape('0', '0', 0x0664, 0x0661));

    for (String invalidEscape : invalidEscapes) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> parseFilterExpression("col(\"" + invalidEscape + "\") == \"x\""));
      Assertions.assertTrue(error.getMessage().contains("invalid Unicode escape"));
    }
  }

  @Test
  void testResourceLimits() {
    String sourcePrefix = "filter := true";
    String maximumSource =
        sourcePrefix + " ".repeat(16 * 1024 - sourcePrefix.getBytes(StandardCharsets.UTF_8).length);
    Assertions.assertDoesNotThrow(
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource + " "));

    String maximumString = "a".repeat(4 * 1024);
    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") == \"" + maximumString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") == \"" + maximumString + "a\""));

    String maximumUnicodeString = "😀".repeat(1024);
    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") == \"" + maximumUnicodeString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") == \"" + maximumUnicodeString + "😀\""));

    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") in " + literalArray(256)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") in " + literalArray(257)));

    Assertions.assertDoesNotThrow(
        () ->
            parseFilterExpression(
                "col(\"left\") in "
                    + literalArray(128)
                    + " or col(\"right\") in "
                    + literalArray(128)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            parseFilterExpression(
                "col(\"left\") in "
                    + literalArray(128)
                    + " or col(\"right\") in "
                    + literalArray(129)));

    String maximumNumber = "1" + "0".repeat(255);
    Assertions.assertDoesNotThrow(
        () -> parseFilterExpression("col(\"value\") == " + maximumNumber));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> parseFilterExpression("col(\"value\") == " + maximumNumber + "0"));

    Assertions.assertDoesNotThrow(() -> parseFilterExpression(balancedAndExpression(0, 85)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> parseFilterExpression(balancedAndExpression(0, 86)));
  }

  @Test
  void testChecksLoweredRowFilterArrayElementCountAtSaveTime() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.parseRowFilter(
                "filter := true if col(\"value\") in " + literalArray(128) + " else := false"));

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := true if col(\"value\") in " + literalArray(129) + " else := false"));
    Assertions.assertTrue(error.getMessage().contains("lowered row-filter literal arrays"));
  }

  @ParameterizedTest(name = "{index}: {0} {1}")
  @MethodSource("restrictedRegoCases")
  void testRestrictedRegoCases(String expectation, String source, String expectedMessage) {
    if (expectation.equals("valid")) {
      Assertions.assertDoesNotThrow(() -> RestrictedRegoExpressionParserFacade.parse(source));
      return;
    }

    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RestrictedRegoExpressionParserFacade.parse(source));
    Assertions.assertTrue(
        error.getMessage().contains(expectedMessage),
        () ->
            "Expected message containing '" + expectedMessage + "' but was: " + error.getMessage());
  }

  @Test
  void testParsesDocumentedPrograms() throws IOException {
    List<String> programs = documentedPrograms(repositoryRoot().resolve("docs/policies.md"));

    Assertions.assertEquals(2, programs.size());
    for (String program : programs) {
      Assertions.assertDoesNotThrow(() -> RestrictedRegoExpressionParserFacade.parse(program));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "col(\"owner\") == session_user()",
        "session_user() != col(\"owner\")",
        "session_user() == \"alice\"",
        "\"alice\" != session_user()",
        "session_user() in [\"alice\", \"bob\"]",
        "is_group_member(\"auditors\")",
        "col(\"region\") in [\"US\", \"CA\"]",
        "not col(\"deleted\") == true",
        "col(\"level\") >= 3 and col(\"region\") == \"US\"",
        "col(\"nullable\") == null",
        "null != col(\"nullable\")",
        "true",
        "false"
      })
  void testAcceptsAllowlistedExpressions(String expression) {
    Assertions.assertDoesNotThrow(() -> parseFilterExpression(expression));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "col(\"owner\")",
        "session_user()",
        "\"alice\"",
        "1",
        "null",
        "[\"US\"]",
        "is_group_member(\"\")",
        "is_group_member(\"auditors\") == true",
        "col(\"left\") == col(\"right\")",
        "1 == 1",
        "col(\"active\") > true",
        "false <= col(\"active\")",
        "session_user() < \"bob\"",
        "session_user() == true",
        "session_user() == session_user()",
        "col(\"region\") in []",
        "col(\"region\") in [\"US\", 1]",
        "col(\"region\") in [\"US\", null]",
        "col(\"region\") == [\"US\"]",
        "col(\"level\") + 1 > 3",
        "col(\"level\") == 1e3",
        "col(\"level\") == +1",
        "col(\"level\") == 01",
        "col(\"a\") < 2 < 3",
        "notcol(\"active\") == true",
        "trueand false",
        "col(\"level\") == 1and col(\"region\") == \"US\"",
        "session_userx() == \"alice\"",
        "col(\"owner\") == \"alice\" trailing",
        "identity(\"user\") == col(\"owner\")",
        "input.user == \"alice\"",
        "col(\"x\") == \"unterminated",
        "col(\"x\") == \"line\nbreak\""
      })
  void testRejectsUnsupportedOrMalformedExpressions(String expression) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> parseFilterExpression(expression));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "true",
        "result := true",
        "package restrictions",
        "import data.rules",
        "filter := true filter := false",
        "filter := true { true }",
        "filter := true # comment",
        "filter := true if is_group_member(\"a\")",
        "filter := true if true else := false if false",
        "filter := true then false else := false",
        "filter := \"not-boolean\"",
        "mask := \"show-last-4\"",
        "mask := action(\"unknown\")",
        "mask := action(\"show-last-4\") if true",
        "mask := action(\"show-last-4\") if col(\"region\") == \"US\" else := action(\"mask-alphanum\")",
        "mask := action(\"show-last-4\") if true else := action(\"mask-alphanum\") if col(\"x\") == 1 else := action(\"replace-with-null\")",
        "mask := true",
        "filter := action(\"show-last-4\")"
      })
  void testRejectsUnsupportedOrMalformedPrograms(String source) {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RestrictedRegoExpressionParserFacade.parse(source));
  }

  @Test
  void testReportsSpecificOneBasedSyntaxLocations() {
    IllegalArgumentException identifierError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter("filter := region == \"US\""));
    Assertions.assertTrue(identifierError.getMessage().contains("line 1, column 11"));
    Assertions.assertTrue(identifierError.getMessage().contains("unsupported identifier region"));

    IllegalArgumentException characterError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RestrictedRegoExpressionParserFacade.parseRowFilter("filter := @"));
    Assertions.assertTrue(characterError.getMessage().contains("line 1, column 11"));

    IllegalArgumentException leadingZeroError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter("filter := col(\"x\") == 01"));
    Assertions.assertTrue(leadingZeroError.getMessage().contains("leading zeros"));

    IllegalArgumentException chainedComparisonError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoExpressionParserFacade.parseRowFilter(
                    "filter := col(\"x\") == 1 == true"));
    Assertions.assertTrue(
        chainedComparisonError.getMessage().contains("chained comparisons are not supported"));
  }

  private static Stream<Arguments> restrictedRegoCases() throws IOException {
    InputStream input =
        Objects.requireNonNull(
            TestRestrictedRegoExpressionParserFacade.class.getResourceAsStream(
                "/restricted-rego-v1-cases.txt"),
            "restricted Rego test cases");
    List<Arguments> cases = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank() || line.startsWith("#")) {
          continue;
        }
        String[] fields = line.split("\\t", -1);
        Assertions.assertTrue(
            fields.length == 2 || fields.length == 3, "Invalid restricted Rego test case: " + line);
        cases.add(Arguments.of(fields[0], fields[1], fields.length == 3 ? fields[2] : ""));
      }
    }
    return cases.stream();
  }

  private static Path repositoryRoot() {
    Path current = Path.of("").toAbsolutePath();
    while (current != null) {
      if (Files.isRegularFile(current.resolve("docs/policies.md"))) {
        return current;
      }
      current = current.getParent();
    }
    throw new IllegalStateException(
        "Cannot locate repository root from the test working directory");
  }

  private static List<String> documentedPrograms(Path policiesDocument) throws IOException {
    List<String> programs = new ArrayList<>();
    StringBuilder current = null;
    for (String line : Files.readAllLines(policiesDocument, StandardCharsets.UTF_8)) {
      if (line.equals("```restricted-rego-v1")) {
        Assertions.assertNull(current, "Nested restricted Rego documentation fence");
        current = new StringBuilder();
      } else if (current != null && line.equals("```")) {
        programs.add(current.toString().stripTrailing());
        current = null;
      } else if (current != null) {
        current.append(line).append('\n');
      }
    }
    Assertions.assertNull(current, "Unclosed restricted Rego documentation fence");
    return programs;
  }

  private static String unicodeEscape(int first, int second, int third, int fourth) {
    return "\\u"
        + new String(new char[] {(char) first, (char) second, (char) third, (char) fourth});
  }

  private static CanonicalExpression parseFilterExpression(String expression) {
    return RestrictedRegoExpressionParserFacade.parseRowFilter("filter := " + expression)
        .fallback();
  }

  private static String literalArray(int elementCount) {
    StringBuilder result = new StringBuilder("[");
    for (int index = 0; index < elementCount; index++) {
      if (index > 0) {
        result.append(',');
      }
      result.append(index);
    }
    return result.append(']').toString();
  }

  private static String balancedAndExpression(int start, int end) {
    if (end - start == 1) {
      return "col(\"column_" + start + "\") == " + start;
    }

    int middle = start + (end - start) / 2;
    return "("
        + balancedAndExpression(start, middle)
        + " and "
        + balancedAndExpression(middle, end)
        + ")";
  }

  private static String conditionalFilterWithBranches(int branchCount) {
    StringBuilder source = new StringBuilder();
    for (int index = 0; index < branchCount; index++) {
      source
          .append(index == 0 ? "filter := " : " else := ")
          .append("true if col(\"c")
          .append(index)
          .append("\") == 1");
    }
    return source.append(" else := false").toString();
  }
}
