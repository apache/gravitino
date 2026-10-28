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
package org.apache.gravitino.dto.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.semantic.SemanticQueryFilter;
import org.apache.gravitino.semantic.SemanticQueryFilter.Operator;
import org.junit.jupiter.api.Test;

public class TestSemanticQueryFilterDTO {

  @Test
  public void testRoundTrip() {
    SemanticQueryFilter filter = SemanticQueryFilter.of("order_date", Operator.GE, "2024-01-01");

    SemanticQueryFilterDTO dto = SemanticQueryFilterDTO.fromFilter(filter);

    assertEquals("order_date", dto.getField());
    assertEquals("ge", dto.getOperator());
    assertEquals("2024-01-01", dto.getValue());
    dto.validate();
    assertEquals(filter, dto.toFilter());
  }

  @Test
  public void testOperatorIsParsedCaseInsensitively() {
    for (Operator operator : Operator.values()) {
      SemanticQueryFilterDTO dto =
          new SemanticQueryFilterDTO("field", operator.name().toLowerCase(), "value");
      assertEquals(operator, dto.toFilter().operator());
    }
  }

  @Test
  public void testValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticQueryFilterDTO("", "eq", "value").validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticQueryFilterDTO("field", "", "value").validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticQueryFilterDTO("field", "eq", null).validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticQueryFilterDTO("field", "between", "value").validate());
  }
}
