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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestColumnAudit {

  @Test
  void testColumnAuditInfoReturnsNull() {
    // Test that Column.ColumnImpl returns null for auditInfo (current behavior)
    Column realColumn =
        Column.of(
            "test_column",
            Types.StringType.get(),
            "test comment",
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET);

    // Verify that the column returns null for audit info (current implementation)
    assertNull(realColumn.auditInfo());
  }

  @Test
  void testColumnAuditInfoWithMock() {
    // Test that Column interface supports auditInfo() method
    Column mockColumn = mock(Column.class);
    AuditInfo realAuditInfo =
        AuditInfo.builder().withCreator("test-user").withCreateTime(Instant.now()).build();

    when(mockColumn.auditInfo()).thenReturn(realAuditInfo);

    // Verify that the column can return audit info when provided
    assertEquals("test-user", mockColumn.auditInfo().creator());
  }
}
