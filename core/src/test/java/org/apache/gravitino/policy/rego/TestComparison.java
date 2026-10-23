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

/** Tests for {@link Comparison}. */
public class TestComparison {

  @Test
  void testValidationAndStructuralEquality() {
    Comparison comparison =
        new Comparison(
            ComparisonOperator.EQ, new Column("owner"), new Literal(LiteralType.STRING, "alice"));
    Comparison equal =
        new Comparison(
            ComparisonOperator.EQ, new Column("owner"), new Literal(LiteralType.STRING, "alice"));

    Assertions.assertDoesNotThrow(comparison::validate);
    Assertions.assertEquals(1, comparison.depth());
    Assertions.assertEquals(comparison, equal);
    Assertions.assertEquals(comparison.hashCode(), equal.hashCode());
  }

  @Test
  void testCompactSemanticError() {
    Comparison invalid =
        new Comparison(
            ComparisonOperator.LT, new Column("active"), new Literal(LiteralType.BOOLEAN, true));

    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, invalid::validate);
    Assertions.assertTrue(error.getMessage().contains("left=column"));
    Assertions.assertTrue(error.getMessage().contains("right=literal(BOOLEAN)"));
    Assertions.assertFalse(error.getMessage().contains("value=true"));
  }
}
