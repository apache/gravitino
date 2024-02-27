/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperationDispatcher;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.dto.requests.TableCreateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdateRequest;
import com.datastrato.gravitino.dto.requests.TableUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

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
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  private DistributionDTO createMockDistributionDTO(String columnName, int bucketNum) {
    return new DistributionDTO.Builder()
        .withStrategy(Strategy.HASH)
        .withNumber(bucketNum)
        .withArgs(FieldReferenceDTO.of(columnName))
        .build();
  }

  private SortOrderDTO[] createMockSortOrderDTO(String columnName, SortDirection direction) {
    return new SortOrderDTO[] {
      new SortOrderDTO.Builder()
          .withDirection(direction)
          .withNullOrder(NullOrdering.NULLS_FIRST)
          .withSortTerm(FieldReferenceDTO.of(columnName))
          .build()
    };
  }

  @Test
  public void testCreateTable() {
    Column[] columns =
        new Column[] {
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    Table table = mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"));
    when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(table);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", SortDirection.DESCENDING);
    DistributionDTO distributionDTO = createMockDistributionDTO("col2", 10);
    TableCreateRequest req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            sortOrderDTOs,
            distributionDTO,
            Partitioning.EMPTY_PARTITIONING,
            IndexDTO.EMPTY_INDEXES);

    Response resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
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

    // Test bad column name
    sortOrderDTOs = createMockSortOrderDTO("col_1", SortDirection.DESCENDING);
    distributionDTO = createMockDistributionDTO("col_2", 10);

    TableCreateRequest badReq =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            sortOrderDTOs,
            distributionDTO,
            Partitioning.EMPTY_PARTITIONING,
            IndexDTO.EMPTY_INDEXES);

    resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(badReq, MediaType.APPLICATION_JSON_TYPE));

    // Sort order and distribution columns name are not found in table columns
    Assertions.assertEquals(Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    Assertions.assertNotNull(tableDTO.partitioning());
    Assertions.assertEquals(0, tableDTO.partitioning().length);
    Assertions.assertEquals(DistributionDTO.NONE, tableDTO.distribution());
    Assertions.assertEquals(0, tableDTO.sortOrder().length);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any(), any());

    Response resp1 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw TableAlreadyExistsException
    doThrow(new TableAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any(), any());

    Response resp2 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        TableAlreadyExistsException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(dispatcher)
        .createTable(any(), any(), any(), any(), any(), any(), any(), any());

    Response resp3 =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
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
          mockColumn("col1", Types.StringType.get()),
          mockColumn("col2", Types.ByteType.get()),
          mockColumn("col3", Types.IntegerType.get(), "test", false, false)
        };
    Partitioning[] partitioning =
        new Partitioning[] {IdentityPartitioningDTO.of(columns[0].name())};
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), partitioning);
    when(dispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(table);

    TableCreateRequest req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            partitioning,
            IndexDTO.EMPTY_INDEXES);

    Response resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    TableResponse tableResp = resp.readEntity(TableResponse.class);
    Assertions.assertEquals(0, tableResp.getCode());

    TableDTO tableDTO = tableResp.getTable();
    Assertions.assertEquals("table1", tableDTO.name());
    Assertions.assertEquals("mock comment", tableDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), tableDTO.properties());

    Column[] columnDTOs = tableDTO.columns();
    Assertions.assertEquals(3, columnDTOs.length);
    Assertions.assertEquals(columns[0].name(), columnDTOs[0].name());
    Assertions.assertEquals(columns[0].dataType(), columnDTOs[0].dataType());
    Assertions.assertEquals(columns[0].comment(), columnDTOs[0].comment());
    Assertions.assertEquals(columns[0].autoIncrement(), columnDTOs[0].autoIncrement());

    Assertions.assertEquals(columns[1].name(), columnDTOs[1].name());
    Assertions.assertEquals(columns[1].dataType(), columnDTOs[1].dataType());
    Assertions.assertEquals(columns[1].comment(), columnDTOs[1].comment());
    Assertions.assertEquals(columns[1].autoIncrement(), columnDTOs[1].autoIncrement());

    Assertions.assertEquals(columns[2].name(), columnDTOs[2].name());
    Assertions.assertEquals(columns[2].dataType(), columnDTOs[2].dataType());
    Assertions.assertEquals(columns[2].comment(), columnDTOs[2].comment());
    Assertions.assertEquals(columns[2].autoIncrement(), columnDTOs[2].autoIncrement());

    Assertions.assertArrayEquals(partitioning, tableDTO.partitioning());

    // Test partition field not exist
    Partitioning errorPartition = IdentityPartitioningDTO.of("not_exist_field");

    req =
        new TableCreateRequest(
            "table1",
            "mock comment",
            Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new),
            ImmutableMap.of("k1", "v1"),
            SortOrderDTO.EMPTY_SORT,
            null,
            new Partitioning[] {errorPartition},
            IndexDTO.EMPTY_INDEXES);
    resp =
        target(tablePath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    when(dispatcher.loadTable(any())).thenReturn(table);

    Response resp =
        target(tablePath(metalake, catalog, schema) + "table1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    Table table = mockTable("table1", columns, "mock comment", ImmutableMap.of(), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testAddTableColumnFirst() {
    TableUpdateRequest.AddTableColumnRequest req =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"col3"},
            Types.StringType.get(),
            "mock comment",
            TableChange.ColumnPosition.first(),
            false,
            false);
    Column[] columns =
        new Column[] {
          mockColumn("col3", Types.StringType.get(), false),
          mockColumn("col1", Types.StringType.get()),
          mockColumn("col2", Types.ByteType.get())
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
            Types.StringType.get(),
            "mock comment",
            TableChange.ColumnPosition.after("col2"),
            true,
            false);
    Column[] columns =
        new Column[] {
          mockColumn("col1", Types.StringType.get()),
          mockColumn("col2", Types.ByteType.get()),
          mockColumn("col3", Types.StringType.get())
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
          mockColumn("col3", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateTableColumnType() {
    TableUpdateRequest.UpdateTableColumnTypeRequest req =
        new TableUpdateRequest.UpdateTableColumnTypeRequest(
            new String[] {"col1"}, Types.ByteType.get());
    Column[] columns =
        new Column[] {
          mockColumn("col1", Types.ByteType.get()), mockColumn("col2", Types.ByteType.get())
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
          mockColumn("col1", Types.StringType.get()), mockColumn("col2", Types.ByteType.get())
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
          mockColumn("col2", Types.ByteType.get()), mockColumn("col1", Types.StringType.get())
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testAddTableIndex() {
    TableUpdateRequest.AddTableIndexRequest req =
        new TableUpdateRequest.AddTableIndexRequest(
            Index.IndexType.PRIMARY_KEY,
            Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME,
            new String[][] {{"col1"}});
    Column[] columns =
        new Column[] {
          mockColumn("col2", Types.ByteType.get()), mockColumn("col1", Types.StringType.get())
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testDeleteTableIndex() {
    TableUpdateRequest.DeleteTableIndexRequest req =
        new TableUpdateRequest.DeleteTableIndexRequest("test", false);
    Column[] columns =
        new Column[] {
          mockColumn("col2", Types.ByteType.get()), mockColumn("col1", Types.StringType.get())
        };
    Table table =
        mockTable("table1", columns, "mock comment", ImmutableMap.of("k1", "v1"), new Transform[0]);
    testAlterTableRequest(req, table);
  }

  @Test
  public void testUpdateColumnAutoIncrement() {
    TableUpdateRequest.UpdateColumnAutoIncrementRequest req =
        new TableUpdateRequest.UpdateColumnAutoIncrementRequest(new String[] {"test"}, false);
    Column[] columns =
        new Column[] {
          mockColumn("col2", Types.ByteType.get()), mockColumn("col1", Types.StringType.get())
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
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testPurgeTable() {
    when(dispatcher.purgeTable(any())).thenReturn(true);

    Response resp =
        target(tablePath(metalake, catalog, schema) + "table1")
            .queryParam("purge", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test when failed to drop table
    when(dispatcher.purgeTable(any())).thenReturn(false);

    Response resp1 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .queryParam("purge", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResponse1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse1.getCode());
    Assertions.assertFalse(dropResponse1.dropped());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).purgeTable(any());

    Response resp2 =
        target(tablePath(metalake, catalog, schema) + "table1")
            .queryParam("purge", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
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
            .accept("application/vnd.gravitino.v1+json")
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

    Assertions.assertEquals(tableDTO.distribution(), updatedTable.distribution());
    Assertions.assertArrayEquals(tableDTO.sortOrder(), updatedTable.sortOrder());
    Assertions.assertArrayEquals(tableDTO.index(), updatedTable.index());
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

  public static Column mockColumn(String name, Type type) {
    return mockColumn(name, type, true);
  }

  private static Column mockColumn(String name, Type type, boolean nullable) {
    return mockColumn(name, type, "mock comment", nullable);
  }

  private static Column mockColumn(String name, Type type, String comment, boolean nullable) {
    Column column = mock(Column.class);
    when(column.name()).thenReturn(name);
    when(column.dataType()).thenReturn(type);
    when(column.comment()).thenReturn(comment);
    when(column.nullable()).thenReturn(nullable);
    return column;
  }

  private static Column mockColumn(
      String name, Type type, String comment, boolean nullable, boolean autoIncrement) {
    Column column = mock(Column.class);
    when(column.name()).thenReturn(name);
    when(column.dataType()).thenReturn(type);
    when(column.comment()).thenReturn(comment);
    when(column.nullable()).thenReturn(nullable);
    when(column.autoIncrement()).thenReturn(autoIncrement);
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
      Distribution distribution,
      SortOrder[] sortOrder) {
    Table table = mockTable(tableName, columns, comment, properties, new Transform[0]);
    when(table.distribution()).thenReturn(distribution);
    when(table.sortOrder()).thenReturn(sortOrder);

    return table;
  }

  public static Table mockTable(
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
    when(table.sortOrder()).thenReturn(new SortOrder[0]);
    when(table.distribution()).thenReturn(DistributionDTO.NONE);
    when(table.index()).thenReturn(Indexes.EMPTY_INDEXES);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(table.auditInfo()).thenReturn(mockAudit);

    return table;
  }
}
