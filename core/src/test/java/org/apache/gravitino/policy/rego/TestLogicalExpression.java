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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link LogicalExpression}. */
public class TestLogicalExpression {

  @Test
  void testFlattensAssociativeBooleanChains() {
    Literal value = new Literal(LiteralType.BOOLEAN, true);
    CanonicalExpression nested = LogicalExpression.of(LogicalOperator.AND, List.of(value, value));
    LogicalExpression flattened =
        (LogicalExpression) LogicalExpression.of(LogicalOperator.AND, List.of(value, nested));

    Assertions.assertEquals(3, flattened.operands().size());
    Assertions.assertEquals(2, flattened.depth());
    Assertions.assertDoesNotThrow(flattened::validate);
    Assertions.assertEquals("and", flattened.operator().sourceToken());
  }

  @Test
  void testReturnsSingleOperandWithoutWrapper() {
    Literal value = new Literal(LiteralType.BOOLEAN, true);
    Assertions.assertSame(value, LogicalExpression.of(LogicalOperator.OR, List.of(value)));
  }

  @Test
  void testRejectsInvalidOperands() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new LogicalExpression(
                    LogicalOperator.OR, List.of(new Literal(LiteralType.BOOLEAN, true)))
                .validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new LogicalExpression(
                    LogicalOperator.OR,
                    List.of(
                        new Literal(LiteralType.BOOLEAN, true),
                        new Literal(LiteralType.STRING, "not a predicate")))
                .validate());
  }
}
