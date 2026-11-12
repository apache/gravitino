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
package org.apache.gravitino.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.DistributionDTO;
import org.apache.gravitino.dto.rel.SortOrderDTO;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Creating a table that replaces an existing Lance table needs MODIFY_TABLE, not CREATE_TABLE. */
public class TestTableOperationsOverwriteAuthorization {

  private static final NameIdentifier TABLE =
      NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");

  private final TableDispatcher dispatcher = mock(TableDispatcher.class);
  private TableOperations operations;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    reset(dispatcher);
    Table table = mock(Table.class);
    when(table.name()).thenReturn("table1");
    when(table.columns()).thenReturn(new Column[0]);
    when(table.properties()).thenReturn(ImmutableMap.of());
    Audit audit = mock(Audit.class);
    when(audit.creator()).thenReturn("user1");
    when(audit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(audit);
    when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(table);
    operations = new TableOperations(dispatcher);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
  }

  @Test
  public void testOverwriteWithoutModifyPrivilegeIsForbidden() {
    for (String mode : new String[] {"OVERWRITE", "overwrite", " OverWrite "}) {
      try (MockedStatic<MetadataAuthzHelper> authz = mockStatic(MetadataAuthzHelper.class)) {
        allowModifyTable(authz, false);

        Response response = create(ImmutableMap.of("lance.creation-mode", mode));

        Assertions.assertEquals(
            Response.Status.FORBIDDEN.getStatusCode(), response.getStatus(), mode);
        ErrorResponse error = (ErrorResponse) response.getEntity();
        Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, error.getCode());
        Assertions.assertTrue(error.getMessage().contains("overwrite"), error.getMessage());
        verify(dispatcher, never())
            .createTable(any(), any(), any(), any(), any(), any(), any(), any());
      }
    }
  }

  @Test
  public void testOverwriteWithModifyPrivilegeCreatesTheTable() {
    try (MockedStatic<MetadataAuthzHelper> authz = mockStatic(MetadataAuthzHelper.class)) {
      allowModifyTable(authz, true);

      Response response =
          create(ImmutableMap.of("lance.creation-mode", "OVERWRITE", "format", "lance"));

      Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      authz.verify(
          () ->
              MetadataAuthzHelper.checkAccess(
                  eq(TABLE),
                  eq(Entity.EntityType.TABLE),
                  eq(AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION)),
          times(1));
      verify(dispatcher, times(1))
          .createTable(eq(TABLE), any(), any(), any(), any(), any(), any(), any());
    }
  }

  @Test
  public void testCreateWithoutOverwriteNeedsNoModifyPrivilege() {
    Map<String, String>[] cases =
        new Map[] {
          ImmutableMap.of(),
          ImmutableMap.of("lance.creation-mode", "CREATE"),
          ImmutableMap.of("lance.creation-mode", "EXIST_OK")
        };
    for (Map<String, String> properties : cases) {
      try (MockedStatic<MetadataAuthzHelper> authz = mockStatic(MetadataAuthzHelper.class)) {
        allowModifyTable(authz, false);

        Response response = create(properties);

        Assertions.assertEquals(
            Response.Status.OK.getStatusCode(), response.getStatus(), properties.toString());
        authz.verify(() -> MetadataAuthzHelper.checkAccess(any(), any(), any()), never());
      }
    }
  }

  private static void allowModifyTable(MockedStatic<MetadataAuthzHelper> authz, boolean allowed) {
    authz
        .when(
            () ->
                MetadataAuthzHelper.checkAccess(
                    any(),
                    any(Entity.EntityType.class),
                    eq(AuthorizationExpressionConstants.MODIFY_TABLE_AUTHORIZATION_EXPRESSION)))
        .thenReturn(allowed);
  }

  private Response create(Map<String, String> properties) {
    TableCreateRequest request =
        new TableCreateRequest(
            "table1",
            "comment",
            new ColumnDTO[] {
              ColumnDTO.builder().withName("id").withDataType(Types.IntegerType.get()).build()
            },
            properties,
            new SortOrderDTO[0],
            DistributionDTO.NONE,
            Partitioning.EMPTY_PARTITIONING,
            IndexDTO.EMPTY_INDEXES);
    return operations.createTable("metalake1", "catalog1", "schema1", request);
  }
}
