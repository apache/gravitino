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

package org.apache.gravitino.spark.connector.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestAuthorizationTable {

  @AfterEach
  void clearCollector() {
    AuthorizationTable.clear();
  }

  @Test
  void testDenyCollectsAndAggregatesDeniedTables() {
    ForbiddenException firstCause = new ForbiddenException("denied table_a");
    Table table =
        AuthorizationTable.deny(
            "table_a",
            "catalog.schema.table_a",
            ImmutableSet.of(Privilege.Name.SELECT_TABLE),
            firstCause);
    AuthorizationTable.deny(
        "table_b",
        "catalog.schema.table_b",
        ImmutableSet.of(Privilege.Name.MODIFY_TABLE),
        new ForbiddenException("denied table_b"));

    assertTrue(table instanceof SupportsRead);
    assertTrue(table instanceof SupportsWrite);
    assertTrue(table.capabilities().contains(TableCapability.BATCH_READ));
    assertTrue(table.capabilities().contains(TableCapability.BATCH_WRITE));

    Optional<ForbiddenException> failure = AuthorizationTable.drainFailure();
    assertTrue(failure.isPresent());
    assertEquals(
        "Missing required privileges for Spark tables: "
            + "[catalog.schema.table_a: SELECT_TABLE; catalog.schema.table_b: MODIFY_TABLE]",
        failure.get().getMessage());
    assertSame(firstCause, failure.get().getCause());

    // The collector is drained, so a subsequent query starts clean.
    assertFalse(AuthorizationTable.drainFailure().isPresent());
  }

  @Test
  void testFailsClosedWhenAccessed() {
    ForbiddenException cause = new ForbiddenException("denied");
    Table table =
        AuthorizationTable.deny(
            "table_a",
            "catalog.schema.table_a",
            ImmutableSet.of(Privilege.Name.SELECT_TABLE),
            cause);

    assertSame(
        cause,
        assertThrows(ForbiddenException.class, () -> ((SupportsRead) table).newScanBuilder(null)));
    assertSame(
        cause,
        assertThrows(
            ForbiddenException.class, () -> ((SupportsWrite) table).newWriteBuilder(null)));
  }

  @Test
  void testDrainFailureEmptyWhenNothingDenied() {
    assertFalse(AuthorizationTable.drainFailure().isPresent());
  }
}
