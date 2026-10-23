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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

/** Tests for the specialized Doris batch-read table boundary. */
public class TestDorisReadOnlyTable35 {

  @Test
  void testOnlyBatchReadIsExposed() {
    Table table = new DorisReadOnlyTable35(mock(SparkJdbcTable.class));

    assertEquals(Set.of(TableCapability.BATCH_READ), table.capabilities());
    assertFalse(table instanceof SupportsWrite);
  }

  @Test
  void testOrdinaryReadOptionsReachTheJdbcDelegate() {
    SparkJdbcTable jdbcTable = mock(SparkJdbcTable.class);
    JDBCScanBuilder scanBuilder = mock(JDBCScanBuilder.class);
    CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Map.of("fetchsize", "100"));
    when(jdbcTable.newScanBuilder(options)).thenReturn(scanBuilder);

    assertSame(scanBuilder, new DorisReadOnlyTable35(jdbcTable).newScanBuilder(options));
    verify(jdbcTable).newScanBuilder(options);
  }

  @Test
  void testProtectedScanOptionsAreRejectedBeforeCallingTheDelegate() {
    SparkJdbcTable jdbcTable = mock(SparkJdbcTable.class);
    DorisReadOnlyTable35 table = new DorisReadOnlyTable35(jdbcTable);
    for (String key :
        List.of(
            "URL",
            "password",
            "query",
            "prepareQuery",
            "sessionInitStatement",
            "DBNAME",
            "createDatabaseIfNotExist")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> table.newScanBuilder(new CaseInsensitiveStringMap(Map.of(key, "override"))));
    }
    verifyNoInteractions(jdbcTable);
  }
}
