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

import java.util.List;

/** Shared builders and counters for restricted Rego tests. */
final class RestrictedRegoTestUtils {

  private RestrictedRegoTestUtils() {}

  static CanonicalExpression parseFilterExpression(String expression) {
    return RestrictedRegoExpressionParserFacade.parseRowFilter("filter := " + expression)
        .fallback();
  }

  static String literalArray(int elementCount) {
    StringBuilder result = new StringBuilder("[");
    for (int index = 0; index < elementCount; index++) {
      if (index > 0) {
        result.append(',');
      }
      result.append(index);
    }
    return result.append(']').toString();
  }

  static String balancedAndExpression(int start, int end) {
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

  static String conditionalFilterWithBranches(int branchCount) {
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

  static String unicodeEscape(int first, int second, int third, int fourth) {
    return "\\u"
        + new String(new char[] {(char) first, (char) second, (char) third, (char) fourth});
  }

  static List<String> invalidUnicodeEscapes() {
    return List.of(
        unicodeEscape(0x0660, 0x0660, 0x0664, 0x0661),
        unicodeEscape(0xFF10, 0xFF10, 0xFF14, 0xFF11),
        unicodeEscape(0x0966, 0x0966, 0x096A, 0x0967),
        unicodeEscape('0', '0', 0x0664, 0x0661));
  }

  static int countExpressionNodes(CanonicalExpression expression) {
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return 1 + countExpressionNodes(comparison.left()) + countExpressionNodes(comparison.right());
    }
    if (expression instanceof Not) {
      return 1 + countExpressionNodes(((Not) expression).operand());
    }
    if (expression instanceof LogicalExpression) {
      int count = 1;
      for (CanonicalExpression operand : ((LogicalExpression) expression).operands()) {
        count += countExpressionNodes(operand);
      }
      return count;
    }
    return 1;
  }

  static int countArrayElements(CanonicalExpression expression) {
    if (expression instanceof Comparison) {
      Comparison comparison = (Comparison) expression;
      return countArrayElements(comparison.left()) + countArrayElements(comparison.right());
    }
    if (expression instanceof Not) {
      return countArrayElements(((Not) expression).operand());
    }
    if (expression instanceof LogicalExpression) {
      int count = 0;
      for (CanonicalExpression operand : ((LogicalExpression) expression).operands()) {
        count += countArrayElements(operand);
      }
      return count;
    }
    return expression instanceof LiteralArray ? ((LiteralArray) expression).values().size() : 0;
  }
}
