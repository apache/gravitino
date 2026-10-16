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

import java.math.BigDecimal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link Literal}. */
public class TestLiteral {

  @Test
  void testValidatesSupportedLiteralTypes() {
    Assertions.assertDoesNotThrow(() -> new Literal(LiteralType.STRING, "value").validate());
    Assertions.assertDoesNotThrow(
        () -> new Literal(LiteralType.NUMBER, new BigDecimal("1.0")).validate());
    Assertions.assertDoesNotThrow(() -> new Literal(LiteralType.BOOLEAN, true).validate());
    Assertions.assertDoesNotThrow(() -> new Literal(LiteralType.NULL, null).validate());
  }

  @Test
  void testExactNumberValueSemantics() {
    Literal one = new Literal(LiteralType.NUMBER, new BigDecimal("1"));
    Literal onePointZero = new Literal(LiteralType.NUMBER, new BigDecimal("1.00"));
    Literal negativeZero = new Literal(LiteralType.NUMBER, new BigDecimal("0.00"), true);

    Assertions.assertEquals(one, onePointZero);
    Assertions.assertEquals(one.hashCode(), onePointZero.hashCode());
    Assertions.assertTrue(negativeZero.negativeZero());
    Assertions.assertNotEquals(negativeZero, new Literal(LiteralType.NUMBER, BigDecimal.ZERO));
  }
}
