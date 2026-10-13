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

import java.nio.charset.StandardCharsets;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RuleType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link RestrictedRegoExpressionParserFacade}. */
public class TestRestrictedRegoExpressionParserFacade {

  @Test
  void testParsesTypedRules() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.validateRowFilter(
                "filter := col(\"owner\") == session_user()"));
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoExpressionParserFacade.validateColumnMask(
                "mask := action(\"show-last-4\")"));

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
  void testEnforcesSourceLength() {
    String sourcePrefix = "filter := true";
    String maximumSource =
        sourcePrefix + " ".repeat(16 * 1024 - sourcePrefix.getBytes(StandardCharsets.UTF_8).length);

    Assertions.assertDoesNotThrow(
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoExpressionParserFacade.parseRowFilter(maximumSource + " "));
  }

  @Test
  void testRejectsInvalidUnicodeEscapeDigitsThroughFacade() {
    for (String invalidEscape : RestrictedRegoTestUtils.invalidUnicodeEscapes()) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  RestrictedRegoExpressionParserFacade.validateRowFilter(
                      "filter := col(\"" + invalidEscape + "\") == \"x\""));
      Assertions.assertTrue(error.getMessage().contains("invalid Unicode escape"));
    }
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
}
