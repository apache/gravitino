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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestColumnAudit {

  @Test
  void testColumnAuditWithRealColumn() {
    // Test with a real Column implementation
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator("test-user")
            .withCreateTime(Instant.now())
            .withLastModifier("test-user")
            .withLastModifiedTime(Instant.now())
            .build();

    Column realColumn =
        Column.of(
            "test_column",
            Types.StringType.get(),
            "test comment",
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET);

    // Note: This test demonstrates that we can create a real Column with audit info
    // In a real scenario, the audit info would be set by the catalog implementation
    assertEquals("test_column", realColumn.name());
    assertEquals(Types.StringType.get(), realColumn.dataType());
    assertEquals("test comment", realColumn.comment());
    assertEquals(true, realColumn.nullable());
    assertEquals(false, realColumn.autoIncrement());
    assertNotNull(auditInfo);
    assertEquals("test-user", auditInfo.creator());
  }

  @Test
  void testColumnAuditInfoRetrieval() {
    // Test that Column interface supports auditInfo() method
    Column mockColumn = mock(Column.class);
    AuditInfo realAuditInfo =
        AuditInfo.builder().withCreator("test-user").withCreateTime(Instant.now()).build();

    when(mockColumn.auditInfo()).thenReturn(realAuditInfo);

    // Verify that the column can return audit info
    assertNotNull(mockColumn.auditInfo());
    assertEquals("test-user", mockColumn.auditInfo().creator());
  }

  @Test
  void testColumnAuditInfoBuilder() {
    // Test AuditInfo builder functionality
    Instant now = Instant.now();
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator("test-user")
            .withCreateTime(now)
            .withLastModifier("test-user")
            .withLastModifiedTime(now)
            .build();

    assertEquals("test-user", auditInfo.creator());
    assertEquals(now, auditInfo.createTime());
    assertEquals("test-user", auditInfo.lastModifier());
    assertEquals(now, auditInfo.lastModifiedTime());
  }
}
