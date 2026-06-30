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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestRequiredPrivilegesCheck {

  @AfterEach
  void clearCollector() {
    AuthorizationTable.clear();
  }

  @Test
  void testReportsAllDeniedTablesInDeterministicOrder() {
    AuthorizationTable.deny(
        "table_b",
        "catalog.schema.table_b",
        ImmutableSet.of(Privilege.Name.MODIFY_TABLE),
        new ForbiddenException("denied table_b"));
    AuthorizationTable.deny(
        "table_a",
        "catalog.schema.table_a",
        ImmutableSet.of(Privilege.Name.SELECT_TABLE),
        new ForbiddenException("denied table_a"));
    AuthorizationTable.deny(
        "table_a",
        "catalog.schema.table_a",
        ImmutableSet.of(Privilege.Name.MODIFY_TABLE),
        new ForbiddenException("denied table_a"));

    LogicalPlan plan = new OneRowRelation();
    ForbiddenException exception =
        assertThrows(ForbiddenException.class, () -> new RequiredPrivilegesCheck().apply(plan));

    assertEquals(
        "Missing required privileges for Spark tables: "
            + "[catalog.schema.table_a: MODIFY_TABLE, SELECT_TABLE; "
            + "catalog.schema.table_b: MODIFY_TABLE]",
        exception.getMessage());
  }

  @Test
  void testReturnsPlanUnchangedWhenNothingDenied() {
    LogicalPlan plan = new OneRowRelation();
    assertSame(plan, new RequiredPrivilegesCheck().apply(plan));
  }
}
