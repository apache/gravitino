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
import java.util.Arrays;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

@SuppressWarnings("deprecation") // JavaConverters is deprecated on Scala 2.13 but required on 2.12
public class TestRequiredPrivilegesCheck {

  private static Table denied(
      String tableIdentifier, Privilege.Name first, Privilege.Name... rest) {
    return AuthorizationTable.deny(
        tableIdentifier.substring(tableIdentifier.lastIndexOf('.') + 1),
        tableIdentifier,
        ImmutableSet.<Privilege.Name>builder().add(first).add(rest).build(),
        new ForbiddenException("denied %s", tableIdentifier));
  }

  private static LogicalPlan relation(Table table) {
    return DataSourceV2Relation.create(
        table, scala.Option.empty(), scala.Option.empty(), CaseInsensitiveStringMap.empty());
  }

  @Test
  void testReportsAllDeniedTablesInDeterministicOrder() {
    LogicalPlan plan =
        new Union(
            JavaConverters.asScalaBuffer(
                    Arrays.asList(
                        relation(denied("catalog.schema.table_b", Privilege.Name.MODIFY_TABLE)),
                        relation(
                            denied(
                                "catalog.schema.table_a",
                                Privilege.Name.MODIFY_TABLE,
                                Privilege.Name.SELECT_TABLE))))
                .toList(),
            true,
            true);

    ForbiddenException exception =
        assertThrows(ForbiddenException.class, () -> new RequiredPrivilegesCheck().apply(plan));

    assertEquals(
        "Missing required privileges for Spark tables: "
            + "[catalog.schema.table_a: MODIFY_TABLE, SELECT_TABLE; "
            + "catalog.schema.table_b: MODIFY_TABLE]",
        exception.getMessage());
  }

  @Test
  void testDeniedTableFailsClosedWhenAccessed() {
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
  void testReturnsPlanUnchangedWhenNothingDenied() {
    LogicalPlan plan = new OneRowRelation();
    assertSame(plan, new RequiredPrivilegesCheck().apply(plan));
  }
}
