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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link RestrictedRegoSourceValidator}. */
public class TestRestrictedRegoSourceValidator {

  @Test
  void testSourceDepthLimit() {
    CanonicalExpression depthEight =
        RestrictedRegoTestUtils.parseFilterExpression(
            "not not not not not not not col(\"active\") == true");
    Assertions.assertEquals(RestrictedRegoLimits.MAX_SOURCE_DEPTH, depthEight.depth());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "not not not not not not not not col(\"active\") == true"));
  }

  @Test
  void testSemanticErrorsDescribeOperandKindsWithoutValues() {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RestrictedRegoTestUtils.parseFilterExpression("col(\"b\") < true"));

    Assertions.assertTrue(error.getMessage().contains("boolean literal supports only == and !="));
    Assertions.assertTrue(error.getMessage().contains("operator '<'"));
    Assertions.assertTrue(error.getMessage().contains("left=column"));
    Assertions.assertTrue(error.getMessage().contains("right=literal(BOOLEAN)"));

    IllegalArgumentException largeComparisonError =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RestrictedRegoTestUtils.parseFilterExpression(
                    "session_user() in " + RestrictedRegoTestUtils.literalArray(256)));
    Assertions.assertTrue(largeComparisonError.getMessage().length() < 512);
    Assertions.assertFalse(largeComparisonError.getMessage().contains("value=255"));
  }

  @Test
  void testRejectsInvalidUnicodeScalarsAndNul() {
    String isolatedSurrogate = "\\" + "uD800";
    String nul = "\\" + "u0000";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"" + isolatedSurrogate + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"x\") == \"" + isolatedSurrogate + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoTestUtils.parseFilterExpression("col(\"" + nul + "\") == \"x\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoTestUtils.parseFilterExpression("is_group_member(\"" + nul + "\")"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RestrictedRegoTestUtils.parseFilterExpression("col(\"x\") == \"" + nul + "\""));
  }

  @Test
  void testStringAndNumericLimits() {
    String maximumString = "a".repeat(RestrictedRegoLimits.MAX_STRING_BYTES);
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") == \"" + maximumString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") == \"" + maximumString + "a\""));

    String maximumUnicodeString = "😀".repeat(RestrictedRegoLimits.MAX_STRING_BYTES / 4);
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") == \"" + maximumUnicodeString + "\""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") == \"" + maximumUnicodeString + "😀\""));

    String maximumNumber = "1" + "0".repeat(RestrictedRegoLimits.MAX_NUMERIC_LITERAL_BYTES - 1);
    Assertions.assertDoesNotThrow(
        () -> RestrictedRegoTestUtils.parseFilterExpression("col(\"value\") == " + maximumNumber));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") == " + maximumNumber + "0"));
  }

  @Test
  void testArrayAndAstLimits() {
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") in "
                    + RestrictedRegoTestUtils.literalArray(
                        RestrictedRegoLimits.MAX_ARRAY_ELEMENTS)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"value\") in "
                    + RestrictedRegoTestUtils.literalArray(
                        RestrictedRegoLimits.MAX_ARRAY_ELEMENTS + 1)));

    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"left\") in "
                    + RestrictedRegoTestUtils.literalArray(128)
                    + " or col(\"right\") in "
                    + RestrictedRegoTestUtils.literalArray(128)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                "col(\"left\") in "
                    + RestrictedRegoTestUtils.literalArray(128)
                    + " or col(\"right\") in "
                    + RestrictedRegoTestUtils.literalArray(129)));

    Assertions.assertDoesNotThrow(
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                RestrictedRegoTestUtils.balancedAndExpression(0, 85)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RestrictedRegoTestUtils.parseFilterExpression(
                RestrictedRegoTestUtils.balancedAndExpression(0, 86)));
  }
}
