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
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link LiteralArray}. */
public class TestLiteralArray {

  @Test
  void testValidationAndImmutableValues() {
    Literal value = new Literal(LiteralType.STRING, "US");
    LiteralArray array = new LiteralArray(LiteralType.STRING, List.of(value));

    Assertions.assertDoesNotThrow(array::validate);
    Assertions.assertEquals(LiteralType.STRING, array.elementType());
    Assertions.assertEquals(List.of(value), array.values());
    Assertions.assertThrows(UnsupportedOperationException.class, () -> array.values().add(value));
  }

  @Test
  void testRejectsEmptyAndHeterogeneousArrays() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new LiteralArray(LiteralType.STRING, List.of()).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new LiteralArray(
                    LiteralType.STRING,
                    List.of(
                        new Literal(LiteralType.STRING, "US"),
                        new Literal(LiteralType.NUMBER, BigDecimal.ONE)))
                .validate());
  }
}
