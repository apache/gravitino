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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.JavaConverters;

public class TestRequiredPrivilegesCheck {

  @Test
  void testReportsOneDeniedTable() {
    LogicalPlan plan =
        relation("catalog.schema.table_a", Privilege.Name.SELECT_TABLE, "denied table_a");

    ForbiddenException exception =
        assertThrows(ForbiddenException.class, () -> new RequiredPrivilegesCheck().apply(plan));

    assertEquals(
        "Missing required privileges for Spark tables: " + "[catalog.schema.table_a: SELECT_TABLE]",
        exception.getMessage());
    assertEquals("denied table_a", exception.getCause().getMessage());
  }

  @Test
  void testReportsAllDeniedTablesInDeterministicOrder() {
    LogicalPlan tableB =
        relation("catalog.schema.table_b", Privilege.Name.MODIFY_TABLE, "denied table_b");
    LogicalPlan tableASelect =
        relation("catalog.schema.table_a", Privilege.Name.SELECT_TABLE, "denied table_a");
    LogicalPlan tableAModify =
        relation("catalog.schema.table_a", Privilege.Name.MODIFY_TABLE, "denied table_a");
    Union plan =
        new Union(
            JavaConverters.asScalaIteratorConverter(
                    Arrays.asList(tableB, tableASelect, tableAModify).iterator())
                .asScala()
                .toSeq(),
            false,
            false);

    ForbiddenException exception =
        assertThrows(ForbiddenException.class, () -> new RequiredPrivilegesCheck().apply(plan));

    assertEquals(
        "Missing required privileges for Spark tables: "
            + "[catalog.schema.table_a: MODIFY_TABLE, SELECT_TABLE; "
            + "catalog.schema.table_b: MODIFY_TABLE]",
        exception.getMessage());
  }

  private static LogicalPlan relation(
      String identifier, Privilege.Name privilege, String causeMessage) {
    Table delegate = mock(Table.class);
    when(delegate.name()).thenReturn(identifier);
    when(delegate.schema()).thenReturn(new StructType().add("id", "int"));
    when(delegate.capabilities()).thenReturn(ImmutableSet.of(TableCapability.BATCH_READ));
    Table deniedTable =
        AuthorizationTableProxy.wrap(
            delegate,
            identifier,
            ImmutableSet.of(privilege),
            new ForbiddenException("%s", causeMessage));
    return DataSourceV2Relation.create(deniedTable, Option.empty(), Option.empty());
  }
}
