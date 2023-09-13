/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import static com.datastrato.graviton.dto.rel.PartitionUtils.toPartitions;
import static com.datastrato.graviton.rel.transforms.Transforms.field;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.catalog.CatalogOperationDispatcher;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.dto.rel.DistributionDTO;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.datastrato.graviton.dto.rel.Partition;
import com.datastrato.graviton.dto.rel.SimplePartitionDTO;
import com.datastrato.graviton.dto.rel.SortOrderDTO;
import com.datastrato.graviton.dto.rel.TableDTO;
import com.datastrato.graviton.dto.requests.TableCreateRequest;
import com.datastrato.graviton.dto.requests.TableUpdateRequest;
import com.datastrato.graviton.dto.requests.TableUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.EntityListResponse;
import com.datastrato.graviton.dto.responses.ErrorConstants;
import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.dto.responses.TableResponse;
import com.datastrato.graviton.dto.util.DTOConverters;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.rel.transforms.Transform;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CatalogOperationDispatcher dispatcher = mock(CatalogOperationDispatcher.class);

  private final String metalake = "metalake1";

  private final String catalog = "catalog1";

  private final String schema = "schema1";

  @Override
  protected Application configure() {
    forceSet(TestProperties.CONTAINER_PORT, "0");

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(TableOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(CatalogOperationDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListTables() {
    NameIdentifier table1 = NameIdentifier.of(metalake, catalog, schema, "table1");
    NameIdentifier table2 = NameIdentifier.of(metalake, catalog, schema, "table2");

    when(dispatcher.listTables(any())).thenReturn(new NameIdentifier[] {table1, table2});

    Response resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResp = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    NameIdentifier[] tables = listResp.identifiers();
    Assertions.assertEquals(2, tables.length);
    Assertions.assertEquals(table1, tables[0]);
    Assertions.assertEquals(table2, tables[1]);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).listTables(any());
    Response resp1 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).listTables(any());
    Response resp2 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testCreateTable() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table = mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"));
    when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any())).thenReturn(table);
    SortOrderDTO[] sortOrderDTOS =
        new SortOrderDTO[] {
          new SortOrderDTO.Builder()
              .withDirection(SortOrderDTO.Direction.DESC)
              .withNullOrder(SortOrderDTO.NullOrder.FIRST)
              .withExpression(
                  new FieldExpression.Builder().withFieldName(new String[] {"col1"}).build())
              .build()
        };
    DistributionDTO distributionDTO =
        new DistributionDTO.Builder()
            .withDistMethod(DistributionDTO.DistributionMethod.HASH)
            .withDistNum(10)
            .withExpressions(
                ImmutableList.of(
                    new FieldExpression.Builder().withFieldName(new String[] {"col2"}).build()))
            .build();
    TableCreateRequest req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::fromDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            sortOrderDTOS,
            distributionDTO,
            new Partition[0]);

    Response resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TableResponse tableResp = resp.readEntity(TableResponse.class);
    Assertions.assertEquals(0, tableResp.getCode());

    TableDTO tableDTO = tableResp.getTable();
    Assertions.assertEquals("table1", tableDTO.name());
    Assertions.assertEquals("mock comment", tableDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), tableDTO.properties());

    Column[] columnDTOs = tableDTO.columns();
    Assertions.assertEquals(2, columnDTOs.length);
    Assertions.assertEquals(columns[0].name(), columnDTOs[0].name());
    Assertions.assertEquals(columns[0].dataType(), columnDTOs[0].dataType());
    Assertions.assertEquals(columns[0].comment(), columnDTOs[0].comment());

    Assertions.assertEquals(columns[1].name(), columnDTOs[1].name());
    Assertions.assertEquals(columns[1].dataType(), columnDTOs[1].dataType());
    Assertions.assertEquals(columns[1].comment(), columnDTOs[1].comment());

    sortOrderDTOS =
        new SortOrderDTO[] {
          new SortOrderDTO.Builder()
              .withDirection(SortOrderDTO.Direction.DESC)
              .withNullOrder(SortOrderDTO.NullOrder.FIRST)
              .withExpression(
                  new FieldExpression.Builder().withFieldName(new String[] {"col_1"}).build())
              .build()
        };
    distributionDTO =
        new DistributionDTO.Builder()
            .withDistMethod(DistributionDTO.DistributionMethod.HASH)
            .withDistNum(10)
            .withExpressions(
                ImmutableList.of(
                    new FieldExpression.Builder().withFieldName(new String[] {"col2_2"}).build()))
            .build();

    TableCreateRequest badReq =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::fromDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            sortOrderDTOS,
            distributionDTO,
            new Partition[0]);

    resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(badReq, MediaType.APPLICATION_JSON_TYPE));

    // Sort order and distribution columns name are not found in table columns
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertNotNull(tableDTO.partitioning());
    Assertions.assertEquals(0, tableDTO.partitioning().length);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any());

    Response resp1 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw TableAlreadyExistsException
    doThrow(new TableAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any());

    Response resp2 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        TableAlreadyExistsException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any());

    Response resp3 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testCreatePartitionedTable() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Transform[] transforms = new Transform[] {field(columns[0])};
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), transforms);
    when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any())).thenReturn(table);

    TableCreateRequest req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::fromDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            new SortOrderDTO[0],
            null,
            toPartitions(transforms));

    Response resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TableResponse tableResp = resp.readEntity(TableResponse.class);
    Assertions.assertEquals(0, tableResp.getCode());

    TableDTO tableDTO = tableResp.getTable();
    Assertions.assertEquals("table1", tableDTO.name());
    Assertions.assertEquals("mock comment", tableDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), tableDTO.properties());

    Column[] columnDTOs = tableDTO.columns();
    Assertions.assertEquals(2, columnDTOs.length);
    Assertions.assertEquals(columns[0].name(), columnDTOs[0].name());
    Assertions.assertEquals(columns[0].dataType(), columnDTOs[0].dataType());
    Assertions.assertEquals(columns[0].comment(), columnDTOs[0].comment());

    Assertions.assertEquals(columns[1].name(), columnDTOs[1].name());
    Assertions.assertEquals(columns[1].dataType(), columnDTOs[1].dataType());
    Assertions.assertEquals(columns[1].comment(), columnDTOs[1].comment());

    Assertions.assertArrayEquals(transforms, tableDTO.partitioning());

    // Test partition field not exist
    SimplePartitionDTO errorPartition =
        new SimplePartitionDTO.Builder()
            .withStrategy(Partition.Strategy.IDENTITY)
            .withFieldName(new String[] {"not_exist_field"})
            .build();
    req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::fromDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            new SortOrderDTO[0],
            null,
            new Partition[] {errorPartition});
    resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    ErrorResponse errorResp3 = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testLoadTable() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    when(dispatcher.loadTable(any())).thenReturn(table);

    Response resp =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TableResponse tableResp = resp.readEntity(TableResponse.class);
    Assertions.assertEquals(0, tableResp.getCode());

    TableDTO tableDTO = tableResp.getTable();
    Assertions.assertEquals("table1", tableDTO.name());
    Assertions.assertEquals("mock comment", tableDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), tableDTO.properties());

    Column[] columnDTOs = tableDTO.columns();
    Assertions.assertEquals(2, columnDTOs.length);
    Assertions.assertEquals(columns[0].name(), columnDTOs[0].name());
    Assertions.assertEquals(columns[0].dataType(), columnDTOs[0].dataType());
    Assertions.assertEquals(columns[0].comment(), columnDTOs[0].comment());

    Assertions.assertEquals(columns[1].name(), columnDTOs[1].name());
    Assertions.assertEquals(columns[1].dataType(), columnDTOs[1].dataType());
    Assertions.assertEquals(columns[1].comment(), columnDTOs[1].comment());

    Assertions.assertEquals(0, tableDTO.partitioning().length);

    // Test throw NoSuchTableException
    doThrow(new NoSuchTableException("mock error")).when(dispatcher).loadTable(any());

    Response resp1 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchTableException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).loadTable(any());

    Response resp2 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testRenameTable() {
    TableUpdateRequest.RenameTableRequest req = new TableUpdateRequest.RenameTableRequest("table2");
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table2", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateTableComment() {
    TableUpdateRequest.UpdateTableCommentRequest req =
        new TableUpdateRequest.UpdateTableCommentRequest("new comment");
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "new comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testSetTableProperty() {
    TableUpdateRequest.SetTablePropertyRequest req =
        new TableUpdateRequest.SetTablePropertyRequest("k2", "v2");
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable(
            "table1",
            columns,
            "mock comment",
            ImmutableMap.of("k1", "v1", "k2", "v2"),
            new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testRemoveTableProperty() {
    TableUpdateRequest.RemoveTablePropertyRequest req =
        new TableUpdateRequest.RemoveTablePropertyRequest("k1");
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table = mockTable("table1", columns, "mock comment", ImmutableMap.of(), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testAddTableColumnFirst() {
    TableUpdateRequest.AddTableColumnRequest req =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"col3"},
            TypeCreator.NULLABLE.STRING,
            "mock comment",
            TableChange.ColumnPosition.first());
    Column[] columns =
        new Column[] {
          mockColumn("col3", TypeCreator.NULLABLE.STRING),
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testAddTableColumnAfter() {
    TableUpdateRequest.AddTableColumnRequest req =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"col1"},
            TypeCreator.NULLABLE.STRING,
            "mock comment",
            TableChange.ColumnPosition.after("col2"));
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8),
          mockColumn("col3", TypeCreator.NULLABLE.STRING)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testRenameTableColumn() {
    TableUpdateRequest.RenameTableColumnRequest req =
        new TableUpdateRequest.RenameTableColumnRequest(new String[] {"col1"}, "col3");
    Column[] columns =
        new Column[] {
          mockColumn("col3", TypeCreator.NULLABLE.STRING),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateTableColumnType() {
    TableUpdateRequest.UpdateTableColumnTypeRequest req =
        new TableUpdateRequest.UpdateTableColumnTypeRequest(
            new String[] {"col1"}, TypeCreator.NULLABLE.I8);
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.I8), mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateTableColumnComment() {
    TableUpdateRequest.UpdateTableColumnCommentRequest req =
        new TableUpdateRequest.UpdateTableColumnCommentRequest(
            new String[] {"col1"}, "new comment");
    Column[] columns =
        new Column[] {
          mockColumn("col1", TypeCreator.NULLABLE.STRING, "new comment"),
          mockColumn("col2", TypeCreator.NULLABLE.I8)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateTableColumnPosition() {
    TableUpdateRequest.UpdateTableColumnPositionRequest req =
        new TableUpdateRequest.UpdateTableColumnPositionRequest(
            new String[] {"col1"}, TableChange.ColumnPosition.after("col2"));
    Column[] columns =
        new Column[] {
          mockColumn("col2", TypeCreator.NULLABLE.I8),
          mockColumn("col1", TypeCreator.NULLABLE.STRING)
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testDropTable() {
    when(dispatcher.dropTable(any())).thenReturn(true);

    Response resp =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test when failed to drop table
    when(dispatcher.dropTable(any())).thenReturn(false);

    Response resp1 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResponse1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse1.getCode());
    Assertions.assertFalse(dropResponse1.dropped());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).dropTable(any());

    Response resp2 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  private void testAlterTableRequest(TableUpdateRequest req, Table updatedTable) {
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(ImmutableList.of(req));

    when(dispatcher.alterTable(any(), eq(req.tableChange()))).thenReturn(updatedTable);

    Response resp =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.graviton.v1+json")
            .put(Entity.entity(updatesRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TableResponse tableResp = resp.readEntity(TableResponse.class);
    Assertions.assertEquals(0, tableResp.getCode());

    TableDTO tableDTO = tableResp.getTable();
    Assertions.assertEquals(updatedTable.name(), tableDTO.name());
    Assertions.assertEquals(updatedTable.comment(), tableDTO.comment());
    Assertions.assertEquals(updatedTable.properties(), tableDTO.properties());

    Column[] columnDTOs = tableDTO.columns();
    Assertions.assertEquals(updatedTable.columns().length, columnDTOs.length);

    List<String> expectedColumnNames =
        Arrays.stream(updatedTable.columns()).map(Column::name).collect(Collectors.toList());
    List<String> actualColumnNames =
        Arrays.stream(columnDTOs).map(Column::name).collect(Collectors.toList());
    Assertions.assertEquals(expectedColumnNames, actualColumnNames);

    List<String> expectedColumnComments =
        Arrays.stream(updatedTable.columns()).map(Column::comment).collect(Collectors.toList());
    List<String> actualColumnComments =
        Arrays.stream(columnDTOs).map(Column::comment).collect(Collectors.toList());
    Assertions.assertEquals(expectedColumnComments, actualColumnComments);

    List<Type> expectedColumnTypes =
        Arrays.stream(updatedTable.columns()).map(Column::dataType).collect(Collectors.toList());
    List<Type> actualColumnTypes =
        Arrays.stream(columnDTOs).map(Column::dataType).collect(Collectors.toList());
    Assertions.assertEquals(expectedColumnTypes, actualColumnTypes);

    Assertions.assertArrayEquals(tableDTO.partitioning(), updatedTable.partitioning());
  }

  private static String tablePath(String metalake, String catalog, String schema) {
    return new StringBuilder()
        .append("/metalakes/")
        .append(metalake)
        .append("/catalogs/")
        .append(catalog)
        .append("/schemas/")
        .append(schema)
        .append("/tables/")
        .toString();
  }

  private static Column mockColumn(String name, Type type) {
    return mockColumn(name, type, "mock comment");
  }

  private static Column mockColumn(String name, Type type, String comment) {
    Column column = mock(Column.class);
    when(column.name()).thenReturn(name);
    when(column.dataType()).thenReturn(type);
    when(column.comment()).thenReturn(comment);
    return column;
  }

  private static Table mockTable(
      String tableName, Column[] columns, String comment, Map<String, String> properties) {
    return mockTable(tableName, columns, comment, properties, new Transform[0]);
  }

  private static Table mockTable(
      String tableName,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] transforms) {
    Table table = mock(Table.class);
    when(table.name()).thenReturn(tableName);
    when(table.columns()).thenReturn(columns);
    when(table.comment()).thenReturn(comment);
    when(table.properties()).thenReturn(properties);
    when(table.partitioning()).thenReturn(transforms);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("graviton");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(mockAudit);

    return table;
  }
}
