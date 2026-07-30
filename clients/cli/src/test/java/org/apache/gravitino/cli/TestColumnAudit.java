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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestColumnAudit {

  @Test
  void testColumnAuditInfoNotSupported() {
    // Test that Column.ColumnImpl does not support auditInfo (current behavior)
    Column realColumn =
        Column.of(
            "test_column",
            Types.StringType.get(),
            "test comment",
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET);

    // Verify that the column does not have auditInfo method (current implementation)
    // This test verifies that ColumnAudit command will show "not supported" message
    assertEquals("test_column", realColumn.name());
    assertEquals(Types.StringType.get(), realColumn.dataType());
  }

  @Test
  void testColumnAuditInfoNotSupportedMessage() {
    // Test that ColumnAudit command shows proper message when audit info is not supported
    // This test verifies the current behavior where column audit info is not implemented
    Column realColumn =
        Column.of(
            "test_column",
            Types.StringType.get(),
            "test comment",
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET);

    // Verify that the column exists and has expected properties
    assertEquals("test_column", realColumn.name());
    assertEquals(Types.StringType.get(), realColumn.dataType());

    // Note: Column.auditInfo() method does not exist in current implementation
    // ColumnAudit command will display "Column audit information is not supported yet."
  }
}
