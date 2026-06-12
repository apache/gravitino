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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestAuthorizationTable {

  @Test
  void testDelegatesTableMetadataAndRetainsRequiredPrivilegesWithoutDynamicProxy() {
    Table delegate = mock(Table.class);
    StructType schema = new StructType().add("id", "int");
    Set<TableCapability> capabilities = ImmutableSet.of(TableCapability.BATCH_READ);
    ForbiddenException cause = new ForbiddenException("denied");

    when(delegate.name()).thenReturn("table_a");
    when(delegate.schema()).thenReturn(schema);
    when(delegate.capabilities()).thenReturn(capabilities);

    Table table =
        AuthorizationTable.wrap(
            delegate,
            "catalog.schema.table_a",
            ImmutableSet.of(Privilege.Name.SELECT_TABLE),
            cause);

    assertTrue(table instanceof SupportsRequiredPrivileges);
    assertEquals(AuthorizationTable.class, table.getClass());
    assertEquals("table_a", table.name());
    assertSame(schema, table.schema());
    assertSame(capabilities, table.capabilities());

    SupportsRequiredPrivileges deniedTable = (SupportsRequiredPrivileges) table;
    assertEquals("catalog.schema.table_a", deniedTable.tableIdentifier());
    assertEquals(ImmutableSet.of(Privilege.Name.SELECT_TABLE), deniedTable.requiredPrivileges());
    assertSame(cause, deniedTable.forbiddenException());
  }

  @Test
  void testDelegatesWriteBuilderWithoutDynamicProxy() {
    SupportsWrite delegate = mock(SupportsWrite.class);
    LogicalWriteInfo writeInfo = mock(LogicalWriteInfo.class);
    WriteBuilder writeBuilder = mock(WriteBuilder.class);
    when(delegate.newWriteBuilder(writeInfo)).thenReturn(writeBuilder);

    Table table =
        AuthorizationTable.wrap(
            delegate,
            "catalog.schema.table_a",
            ImmutableSet.of(Privilege.Name.MODIFY_TABLE),
            new ForbiddenException("denied"));

    assertTrue(table instanceof SupportsWrite);
    assertEquals(AuthorizationTable.class, table.getClass());
    assertSame(writeBuilder, ((SupportsWrite) table).newWriteBuilder(writeInfo));
  }
}
