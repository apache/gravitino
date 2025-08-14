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

package org.apache.gravitino.dto.rel;

import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionUtils {

  private static ColumnDTO column(String name) {
    return ColumnDTO.builder().withName(name).withDataType(Types.StringType.get()).build();
  }

  @Test
  void testValidateFieldExistence_success() {
    ColumnDTO[] columns = new ColumnDTO[] {column("id"), column("name")};
    String[] field = new String[] {"name"};

    Assertions.assertDoesNotThrow(() -> PartitionUtils.validateFieldExistence(columns, field));
  }

  @Test
  void testValidateFieldExistence_caseInsensitive() {
    ColumnDTO[] columns = new ColumnDTO[] {column("UserId")};
    String[] field = new String[] {"userid"};

    Assertions.assertDoesNotThrow(() -> PartitionUtils.validateFieldExistence(columns, field));
  }

  @Test
  void testValidateFieldExistence_nullColumns() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(null, new String[] {"a"}));
    Assertions.assertEquals("columns cannot be null or empty", ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_emptyFieldName() {
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(new ColumnDTO[] {column("a")}, null));
    Assertions.assertEquals("fieldName cannot be null or empty", ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_fieldNotFound() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(columns, new String[] {"c"}));
    Assertions.assertEquals("Field 'c' not found in table", ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_nestedFieldsNotSupported() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(columns, new String[] {"a", "b"}));
    Assertions.assertEquals(
        "Nested fields are not supported yet. Field name array must contain exactly one element, but got: [a, b]",
        ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_singleElementArray() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    String[] field = new String[] {"a"};

    Assertions.assertDoesNotThrow(() -> PartitionUtils.validateFieldExistence(columns, field));
  }

  @Test
  void testValidateFieldExistence_multipleNestedFieldsNotSupported() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(columns, new String[] {"a", "b", "c"}));
    Assertions.assertEquals(
        "Nested fields are not supported yet. Field name array must contain exactly one element, but got: [a, b, c]",
        ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_emptyFieldNameArray() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.validateFieldExistence(columns, new String[] {}));
    Assertions.assertEquals("fieldName cannot be null or empty", ex.getMessage());
  }

  @Test
  void testValidateFieldExistence_nestedFieldValidationOrder() {
    ColumnDTO[] columns = new ColumnDTO[] {column("a"), column("b")};
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PartitionUtils.validateFieldExistence(
                    columns, new String[] {"nonexistent", "field"}));
    Assertions.assertEquals(
        "Nested fields are not supported yet. Field name array must contain exactly one element, but got: [nonexistent, field]",
        ex.getMessage());
  }
}
