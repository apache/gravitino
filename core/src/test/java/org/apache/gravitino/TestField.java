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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TestField {

  @Test
  void testRequiredFieldCreation() {
    Field field = Field.required("age", Integer.class, "Age of the person");
    assertNotNull(field);
  }

  @Test
  void testOptionalFieldCreation() {
    Field field = Field.optional("email", String.class);
    assertNotNull(field);
  }

  @Test
  void testFieldValidation() {
    Field field = Field.required("name", String.class);

    assertDoesNotThrow(() -> field.validate("John Doe"));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> field.validate(null));
    assertEquals("Field name is required", ex.getMessage());

    ex = assertThrows(IllegalArgumentException.class, () -> field.validate(42));
    assertEquals("Field name is not of type java.lang.String", ex.getMessage());
  }

  @Test
  void testBuilderMissingName() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Field.required(null, Integer.class));
    assertEquals("Field name is required", ex.getMessage());
  }

  @Test
  void testBuilderMissingTypeClass() {
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> Field.optional("weight", null));
    assertEquals("Field type class is required", ex.getMessage());
  }
}
