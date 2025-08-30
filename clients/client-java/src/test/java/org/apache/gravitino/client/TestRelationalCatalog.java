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
package org.apache.gravitino.client;

import static org.apache.gravitino.dto.rel.partitioning.Partitioning.EMPTY_PARTITIONING;
import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;
import static org.apache.gravitino.dto.util.DTOConverters.fromDTOs;
import static org.apache.gravitino.rel.expressions.sorts.SortDirection.DESCENDING;
import static org.apache.hc.core5.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_METHOD_NOT_ALLOWED;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.DistributionDTO;
import org.apache.gravitino.dto.rel.SortOrderDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.expressions.FieldReferenceDTO;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.SchemaCreateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdatesRequest;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.SchemaResponse;
import org.apache.gravitino.dto.responses.TableResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRelationalCatalog extends TestBase {

  protected static Catalog catalog;

  private static GravitinoMetalake metalake;

  protected static final String metalakeName = "testMetalake";

  protected static final String catalogName = "testCatalog";

  private static final String provider = "test";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = TestGravitinoMetalake.createMetalake(client, metalakeName);

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withType(CatalogDTO.Type.RELATIONAL)
            .withProvider(provider)
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "k2"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogCreateRequest catalogCreateRequest =
        new CatalogCreateRequest(
            catalogName,
            CatalogDTO.Type.RELATIONAL,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
    CatalogResponse catalogResponse = new CatalogResponse(mockCatalog);
    buildMockResource(
        Method.POST,
        "/api/metalakes/" + metalakeName + "/catalogs",
        catalogCreateRequest,
        catalogResponse,
        SC_OK);

    catalog =
        metalake.createCatalog(
            catalogName,
            CatalogDTO.Type.RELATIONAL,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
  }

  @Test
  public void testListSchemas() throws JsonProcessingException {
    Namespace schemaNs = Namespace.of(metalakeName, catalogName);
    NameIdentifier schema1 = NameIdentifier.of(schemaNs, "schema1");
    NameIdentifier schema2 = NameIdentifier.of(schemaNs, "schema2");
    String schemaPath = withSlash(RelationalCatalog.formatSchemaRequestPath(schemaNs));

    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {schema1, schema2});
    buildMockResource(Method.GET, schemaPath, null, resp, SC_OK);
    String[] schemas = catalog.asSchemas().listSchemas();

    Assertions.assertEquals(2, schemas.length);
    Assertions.assertEquals(schema1.name(), schemas[0]);
    Assertions.assertEquals(schema2.name(), schemas[1]);

    // Test return empty schema list
    EntityListResponse emptyResp = new EntityListResponse(new NameIdentifier[] {});
    buildMockResource(Method.GET, schemaPath, null, emptyResp, SC_OK);
    String[] emptySchemas = catalog.asSchemas().listSchemas();
    Assertions.assertEquals(0, emptySchemas.length);

    // Test throw NoSuchCatalogException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "catalog not found");
    buildMockResource(Method.GET, schemaPath, null, errorResp, SC_NOT_FOUND);
    SupportsSchemas supportSchemas = catalog.asSchemas();
    Throwable ex =
        Assertions.assertThrows(NoSuchCatalogException.class, () -> supportSchemas.listSchemas());
    Assertions.assertTrue(ex.getMessage().contains("catalog not found"));

    // Test throw RuntimeException
    ErrorResponse errorResp1 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, schemaPath, null, errorResp1, SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> supportSchemas.listSchemas());
    Assertions.assertTrue(ex1.getMessage().contains("internal error"));

    // Test throw unparsed system error
    buildMockResource(Method.GET, schemaPath, null, "unparsed error", SC_BAD_REQUEST);
    Throwable ex2 =
        Assertions.assertThrows(RESTException.class, () -> supportSchemas.listSchemas());
    Assertions.assertTrue(ex2.getMessage().contains("unparsed error"));
  }

  @Test
  public void testCreateSchema() throws JsonProcessingException {
    String schemaName = "schema1";
    String schemaPath =
        withSlash(
            RelationalCatalog.formatSchemaRequestPath(Namespace.of(metalakeName, catalogName)));
    SchemaDTO schema = createMockSchema(schemaName, "comment", Collections.emptyMap());

    SchemaCreateRequest req =
        new SchemaCreateRequest(schemaName, "comment", Collections.emptyMap());
    SchemaResponse resp = new SchemaResponse(schema);
    buildMockResource(Method.POST, schemaPath, req, resp, SC_OK);

    Schema createdSchema =
        catalog.asSchemas().createSchema(schemaName, "comment", Collections.emptyMap());
    Assertions.assertEquals(schemaName, createdSchema.name());
    Assertions.assertEquals("comment", createdSchema.comment());
    Assertions.assertEquals(Collections.emptyMap(), createdSchema.properties());

    // Test throw NoSuchCatalogException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchCatalogException.class.getSimpleName(), "catalog not found");
    buildMockResource(Method.POST, schemaPath, req, errorResp, SC_NOT_FOUND);

    SupportsSchemas schemas = catalog.asSchemas();
    Map<String, String> emptyMap = Collections.emptyMap();
    Throwable ex =
        Assertions.assertThrows(
            NoSuchCatalogException.class,
            () -> schemas.createSchema(schemaName, "comment", emptyMap));
    Assertions.assertTrue(ex.getMessage().contains("catalog not found"));

    // Test throw SchemaAlreadyExistsException
    ErrorResponse errorResp1 =
        ErrorResponse.alreadyExists(
            SchemaAlreadyExistsException.class.getSimpleName(), "schema already exists");
    buildMockResource(Method.POST, schemaPath, req, errorResp1, SC_CONFLICT);

    Throwable ex1 =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> schemas.createSchema(schemaName, "comment", emptyMap));
    Assertions.assertTrue(ex1.getMessage().contains("schema already exists"));
  }

  @Test
  public void testLoadSchema() throws JsonProcessingException {
    NameIdentifier schemaId = NameIdentifier.of("schema1");
    String schemaPath =
        withSlash(
            RelationalCatalog.formatSchemaRequestPath(Namespace.of(metalakeName, catalogName))
                + "/"
                + schemaId.name());
    SchemaDTO schema = createMockSchema("schema1", "comment", Collections.emptyMap());

    SchemaResponse resp = new SchemaResponse(schema);
    buildMockResource(Method.GET, schemaPath, null, resp, SC_OK);

    Schema loadedSchema = catalog.asSchemas().loadSchema(schemaId.name());
    Assertions.assertEquals("schema1", loadedSchema.name());
    Assertions.assertEquals("comment", loadedSchema.comment());
    Assertions.assertEquals(Collections.emptyMap(), loadedSchema.properties());

    // Test throw NoSuchSchemaException
    ErrorResponse errorResp1 =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, schemaPath, null, errorResp1, SC_NOT_FOUND);

    SupportsSchemas schemas = catalog.asSchemas();
    Throwable ex1 =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> schemas.loadSchema(schemaId.name()));
    Assertions.assertTrue(ex1.getMessage().contains("schema not found"));
  }

  @Test
  public void testSetSchemaProperty() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, "schema1");
    SchemaUpdateRequest.SetSchemaPropertyRequest req =
        new SchemaUpdateRequest.SetSchemaPropertyRequest("k1", "v1");
    SchemaDTO expectedSchema = createMockSchema("schema1", "comment", ImmutableMap.of("k1", "v1"));

    testAlterSchema(ident, req, expectedSchema);
  }

  @Test
  public void testRemoveSchemaProperty() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, "schema1");
    SchemaUpdateRequest.RemoveSchemaPropertyRequest req =
        new SchemaUpdateRequest.RemoveSchemaPropertyRequest("k1");
    SchemaDTO expectedSchema = createMockSchema("schema1", "comment", Collections.emptyMap());

    testAlterSchema(ident, req, expectedSchema);
  }

  @Test
  public void testDropSchema() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(metalakeName, catalogName, "schema1");
    String schemaPath =
        withSlash(
            RelationalCatalog.formatSchemaRequestPath(ident.namespace()) + "/" + ident.name());
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, schemaPath, null, resp, SC_OK);

    Assertions.assertTrue(catalog.asSchemas().dropSchema(ident.name(), false));

    // Test with cascade to ture
    DropResponse resp1 = new DropResponse(true);
    buildMockResource(
        Method.DELETE, schemaPath, ImmutableMap.of("cascade", "true"), null, resp1, SC_OK);

    Assertions.assertTrue(catalog.asSchemas().dropSchema(ident.name(), true));

    // Test throw NonEmptySchemaException
    ErrorResponse errorResp =
        ErrorResponse.nonEmpty(
            NonEmptySchemaException.class.getSimpleName(), "schema is not empty");
    buildMockResource(Method.DELETE, schemaPath, null, errorResp, SC_CONFLICT);

    SupportsSchemas schemas = catalog.asSchemas();
    Throwable ex =
        Assertions.assertThrows(
            NonEmptySchemaException.class, () -> schemas.dropSchema(ident.name(), true));

    Assertions.assertTrue(ex.getMessage().contains("schema is not empty"));
  }

  @Test
  public void testListTables() throws JsonProcessingException {
    NameIdentifier table1 = NameIdentifier.of(metalakeName, catalogName, "schema1", "table1");
    NameIdentifier table2 = NameIdentifier.of(metalakeName, catalogName, "schema1", "table2");
    String tablePath = withSlash(RelationalCatalog.formatTableRequestPath(table1.namespace()));

    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {table1, table2});
    buildMockResource(Method.GET, tablePath, null, resp, SC_OK);
    NameIdentifier[] tables = catalog.asTableCatalog().listTables(Namespace.of("schema1"));

    Assertions.assertEquals(2, tables.length);

    NameIdentifier expectedResult1 = NameIdentifier.of("schema1", "table1");
    NameIdentifier expectedResult2 = NameIdentifier.of("schema1", "table2");
    Assertions.assertEquals(expectedResult1, tables[0]);
    Assertions.assertEquals(expectedResult2, tables[1]);

    // Test throw NoSuchSchemaException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, tablePath, null, errorResp, SC_NOT_FOUND);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Namespace namespace1 = Namespace.of("schema1");
    Throwable ex =
        Assertions.assertThrows(
            NoSuchSchemaException.class, () -> tableCatalog.listTables(namespace1));
    Assertions.assertTrue(ex.getMessage().contains("schema not found"));

    // Test throw RuntimeException
    ErrorResponse errorResp1 = ErrorResponse.internalError("runtime exception");
    buildMockResource(Method.GET, tablePath, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> tableCatalog.listTables(namespace1));
    Assertions.assertTrue(ex1.getMessage().contains("runtime exception"));

    // Test throw unparsed system error
    buildMockResource(Method.GET, tablePath, null, "unparsed error", SC_CONFLICT);
    Throwable ex2 =
        Assertions.assertThrows(RuntimeException.class, () -> tableCatalog.listTables(namespace1));
    Assertions.assertTrue(ex2.getMessage().contains("unparsed error"));
  }

  @Test
  public void testCreateTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath = withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace));

    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("col1", Types.ByteType.get(), "comment1"),
          createMockColumn("col2", Types.StringType.get(), "comment2")
        };

    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col2", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            DistributionDTO.NONE,
            sortOrderDTOs);
    TableCreateRequest req =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            sortOrderDTOs,
            DistributionDTO.NONE,
            EMPTY_PARTITIONING,
            IndexDTO.EMPTY_INDEXES);
    TableResponse resp = new TableResponse(expectedTable);
    buildMockResource(Method.POST, tablePath, req, resp, SC_OK);

    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                tableId, fromDTOs(columns), "comment", Collections.emptyMap(), sortOrderDTOs);
    Assertions.assertEquals(expectedTable.name(), table.name());
    Assertions.assertEquals(expectedTable.comment(), table.comment());
    Assertions.assertEquals(expectedTable.properties(), table.properties());

    Assertions.assertEquals(expectedTable.columns().length, table.columns().length);
    Assertions.assertEquals(expectedTable.columns()[0].name(), table.columns()[0].name());
    Assertions.assertEquals(expectedTable.columns()[0].dataType(), table.columns()[0].dataType());
    Assertions.assertEquals(expectedTable.columns()[0].comment(), table.columns()[0].comment());

    Assertions.assertEquals(expectedTable.columns()[1].name(), table.columns()[1].name());
    Assertions.assertEquals(expectedTable.columns()[1].dataType(), table.columns()[1].dataType());
    Assertions.assertEquals(expectedTable.columns()[1].comment(), table.columns()[1].comment());
    assertTableEquals(fromDTO(expectedTable), table);

    // test validate column default value
    Column[] errorColumns =
        new Column[] {
          Column.of("col1", Types.ByteType.get(), "comment1"),
          Column.of(
              "col2",
              Types.StringType.get(),
              "comment2",
              false,
              false,
              LiteralDTO.builder().withValue(null).withDataType(Types.NullType.get()).build())
        };

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Map<String, String> emptyMap = Collections.emptyMap();
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> tableCatalog.createTable(tableId, errorColumns, "comment", emptyMap));
    Assertions.assertEquals(
        "Column cannot be non-nullable with a null default value: col2", exception.getMessage());

    // Test throw NoSuchSchemaException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.POST, tablePath, req, errorResp, SC_NOT_FOUND);

    SortOrder[] sortOrder =
        Arrays.stream(sortOrderDTOs).map(DTOConverters::fromDTO).toArray(SortOrder[]::new);
    Throwable ex =
        Assertions.assertThrows(
            NoSuchSchemaException.class,
            () ->
                tableCatalog.createTable(
                    tableId, fromDTOs(columns), "comment", emptyMap, sortOrder));
    Assertions.assertTrue(ex.getMessage().contains("schema not found"));

    // Test throw TableAlreadyExistsException
    ErrorResponse errorResp1 =
        ErrorResponse.alreadyExists(
            TableAlreadyExistsException.class.getSimpleName(), "table already exists");
    buildMockResource(Method.POST, tablePath, req, errorResp1, SC_CONFLICT);

    Throwable ex1 =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableId, fromDTOs(columns), "comment", emptyMap, sortOrder));
    Assertions.assertTrue(ex1.getMessage().contains("table already exists"));
  }

  @Test
  public void testCreatePartitionedTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath = withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace));

    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("city", Types.IntegerType.get(), "comment1"),
          createMockColumn("dt", Types.DateType.get(), "comment2")
        };

    // Test empty partitioning
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            null,
            SortOrderDTO.EMPTY_SORT);

    TableCreateRequest req =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            EMPTY_PARTITIONING,
            IndexDTO.EMPTY_INDEXES);
    TableResponse resp = new TableResponse(expectedTable);
    buildMockResource(Method.POST, tablePath, req, resp, SC_OK);

    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                tableId, fromDTOs(columns), "comment", Collections.emptyMap(), EMPTY_PARTITIONING);
    assertTableEquals(fromDTO(expectedTable), table);

    // Test partitioning
    Partitioning[] partitioning = {
      IdentityPartitioningDTO.of(columns[0].name()), DayPartitioningDTO.of(columns[1].name())
    };
    expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            partitioning,
            DistributionDTO.NONE,
            SortOrderDTO.EMPTY_SORT);

    req =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            partitioning,
            IndexDTO.EMPTY_INDEXES);
    resp = new TableResponse(expectedTable);
    buildMockResource(Method.POST, tablePath, req, resp, SC_OK);

    table =
        catalog
            .asTableCatalog()
            .createTable(
                tableId, fromDTOs(columns), "comment", Collections.emptyMap(), partitioning);
    assertTableEquals(fromDTO(expectedTable), table);

    // Test throw TableAlreadyExistsException
    ErrorResponse errorResp1 =
        ErrorResponse.alreadyExists(
            TableAlreadyExistsException.class.getSimpleName(), "table already exists");
    buildMockResource(Method.POST, tablePath, req, errorResp1, SC_CONFLICT);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Map<String, String> emptyMap = Collections.emptyMap();
    Throwable ex1 =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableId, fromDTOs(columns), "comment", emptyMap, partitioning));
    Assertions.assertTrue(ex1.getMessage().contains("table already exists"));

    // Test partitioning field not exist in table
    Partitioning[] errorPartitioning = {IdentityPartitioningDTO.of("not_exist_field")};
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tableCatalog.createTable(
                    tableId, fromDTOs(columns), "comment", emptyMap, errorPartitioning));
    Assertions.assertTrue(ex2.getMessage().contains("not found in table"));

    // Test empty columns
    Throwable ex3 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                tableCatalog.createTable(
                    tableId, new Column[0], "comment", emptyMap, errorPartitioning));
    Assertions.assertTrue(
        ex3.getMessage().contains("\"columns\" field is required and cannot be empty"));

    // Test partitioning with assignments
    Partitioning[] partitioningWithAssignments = {
      RangePartitioningDTO.of(
          new String[] {columns[0].name()},
          new RangePartitionDTO[] {
            RangePartitionDTO.builder()
                .withName("p1")
                .withLower(
                    LiteralDTO.builder()
                        .withDataType(Types.IntegerType.get())
                        .withValue("1")
                        .build())
                .withUpper(
                    LiteralDTO.builder()
                        .withDataType(Types.IntegerType.get())
                        .withValue("10")
                        .build())
                .build()
          })
    };
    expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            partitioningWithAssignments,
            DistributionDTO.NONE,
            SortOrderDTO.EMPTY_SORT);

    req =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            partitioningWithAssignments,
            IndexDTO.EMPTY_INDEXES);
    resp = new TableResponse(expectedTable);
    buildMockResource(Method.POST, tablePath, req, resp, SC_OK);

    table =
        catalog
            .asTableCatalog()
            .createTable(
                tableId,
                fromDTOs(columns),
                "comment",
                Collections.emptyMap(),
                partitioningWithAssignments);
    assertTableEquals(fromDTO(expectedTable), table);
  }

  @Test
  public void testCreateIndexTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "index1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath = withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace));

    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("id", Types.IntegerType.get(), "id"),
          createMockColumn("city", Types.IntegerType.get(), "comment1"),
          createMockColumn("dt", Types.DateType.get(), "comment2")
        };
    IndexDTO[] indexDTOs =
        new IndexDTO[] {
          IndexDTO.builder()
              .withIndexType(IndexDTO.IndexType.PRIMARY_KEY)
              .withFieldNames(new String[][] {{"id"}})
              .build(),
          IndexDTO.builder()
              .withName("uk_1")
              .withIndexType(IndexDTO.IndexType.UNIQUE_KEY)
              .withFieldNames(new String[][] {{"dt"}, {"city"}})
              .build()
        };

    // Test create success.
    TableDTO expectedTable =
        createMockTable(
            tableId.name(),
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            DistributionDTO.NONE,
            SortOrderDTO.EMPTY_SORT,
            indexDTOs);

    TableCreateRequest req =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            EMPTY_PARTITIONING,
            indexDTOs);
    TableResponse resp = new TableResponse(expectedTable);
    buildMockResource(Method.POST, tablePath, req, resp, SC_OK);

    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                tableId,
                fromDTOs(columns),
                "comment",
                Collections.emptyMap(),
                EMPTY_PARTITIONING,
                DistributionDTO.NONE,
                SortOrderDTO.EMPTY_SORT,
                indexDTOs);
    assertTableEquals(fromDTO(expectedTable), table);

    // Test throw TableAlreadyExistsException
    ErrorResponse errorResp1 =
        ErrorResponse.alreadyExists(
            TableAlreadyExistsException.class.getSimpleName(), "table already exists");
    buildMockResource(Method.POST, tablePath, req, errorResp1, SC_CONFLICT);

    Map<String, String> emptyMap = Collections.emptyMap();
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Throwable ex1 =
        Assertions.assertThrows(
            TableAlreadyExistsException.class,
            () ->
                tableCatalog.createTable(
                    tableId,
                    fromDTOs(columns),
                    "comment",
                    emptyMap,
                    EMPTY_PARTITIONING,
                    DistributionDTO.NONE,
                    SortOrderDTO.EMPTY_SORT,
                    indexDTOs));
    Assertions.assertTrue(ex1.getMessage().contains("table already exists"));
  }

  private void assertTableEquals(Table expected, Table actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.properties(), actual.properties());
    Assertions.assertEquals(expected.columns().length, actual.columns().length);
    IntStream.range(0, expected.columns().length)
        .forEach(
            i -> {
              Assertions.assertEquals(expected.columns()[i].name(), actual.columns()[i].name());
              Assertions.assertEquals(
                  expected.columns()[i].dataType(), actual.columns()[i].dataType());
              Assertions.assertEquals(
                  expected.columns()[i].comment(), actual.columns()[i].comment());
              Assertions.assertEquals(
                  expected.columns()[i].nullable(), actual.columns()[i].nullable());
              Assertions.assertEquals(
                  expected.columns()[i].autoIncrement(), actual.columns()[i].autoIncrement());
              Assertions.assertEquals(
                  expected.columns()[i].defaultValue(), actual.columns()[i].defaultValue());
              Assertions.assertEquals(
                  expected.columns()[i].auditInfo(), actual.columns()[i].auditInfo());
            });
    Assertions.assertArrayEquals(expected.partitioning(), actual.partitioning());
  }

  @Test
  public void testLoadTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath =
        withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace) + "/" + tableId.name());
    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("col1", Types.ByteType.get(), "comment1"),
          createMockColumn("col2", Types.StringType.get(), "comment2")
        };

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col2", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            new Partitioning[] {IdentityPartitioningDTO.of(columns[0].name())},
            distributionDTO,
            sortOrderDTOs);

    TableResponse resp = new TableResponse(expectedTable);
    buildMockResource(Method.GET, tablePath, null, resp, SC_OK);

    Table table = catalog.asTableCatalog().loadTable(tableId);
    assertTableEquals(fromDTO(expectedTable), table);

    // Test throw NoSuchTableException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchTableException.class.getSimpleName(), "table not found");
    buildMockResource(Method.GET, tablePath, null, errorResp, SC_NOT_FOUND);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Throwable ex =
        Assertions.assertThrows(NoSuchTableException.class, () -> tableCatalog.loadTable(tableId));
    Assertions.assertTrue(ex.getMessage().contains("table not found"));
  }

  @Test
  public void testRenameTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.ByteType.get(), "comment1")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table2",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.RenameTableRequest req =
        new TableUpdateRequest.RenameTableRequest(expectedTable.name());

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testUpdateTableComment() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.ByteType.get(), "comment1")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment2",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.UpdateTableCommentRequest req =
        new TableUpdateRequest.UpdateTableCommentRequest(expectedTable.comment());

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testSetTableProperty() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.ByteType.get(), "comment1")};
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            properties,
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.SetTablePropertyRequest req =
        new TableUpdateRequest.SetTablePropertyRequest("k1", "v1");

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testRemoveTableProperty() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.ByteType.get(), "comment1")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.RemoveTablePropertyRequest req =
        new TableUpdateRequest.RemoveTablePropertyRequest("k1");

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testAddTableColumn() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("col1", Types.ByteType.get(), "comment1"),
          createMockColumn("col2", Types.StringType.get(), "comment2", false)
        };

    DistributionDTO distributionDTO = createMockDistributionDTO("col2", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col2", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);

    TableUpdateRequest.AddTableColumnRequest req =
        new TableUpdateRequest.AddTableColumnRequest(
            new String[] {"col2"},
            Types.StringType.get(),
            "comment2",
            TableChange.ColumnPosition.after("col1"),
            false,
            false,
            null);

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testRenameTableColumn() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("col1", Types.ByteType.get(), "comment1"),
          createMockColumn("col2", Types.StringType.get(), "comment2"),
          createMockColumn("col3", Types.StringType.get(), "comment3")
        };

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col3", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.RenameTableColumnRequest req =
        new TableUpdateRequest.RenameTableColumnRequest(new String[] {"col2"}, "col3");

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testUpdateTableColumnComment() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.ByteType.get(), "comment2")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);

    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.UpdateTableColumnCommentRequest req =
        new TableUpdateRequest.UpdateTableColumnCommentRequest(new String[] {"col1"}, "comment2");

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testUpdateTableColumnDataType() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.StringType.get(), "comment1")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.UpdateTableColumnTypeRequest req =
        new TableUpdateRequest.UpdateTableColumnTypeRequest(
            new String[] {"col1"}, Types.StringType.get());

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testUpdateTableColumnNullability() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col1", Types.StringType.get(), "comment1")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col1", DESCENDING);
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.UpdateTableColumnNullabilityRequest req =
        new TableUpdateRequest.UpdateTableColumnNullabilityRequest(new String[] {"col1"}, true);

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testUpdateTableColumnPosition() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("col1", Types.ByteType.get(), "comment1"),
          createMockColumn("col2", Types.StringType.get(), "comment2")
        };

    DistributionDTO distributionDTO = createMockDistributionDTO("col1", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col2", DESCENDING);
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.UpdateTableColumnPositionRequest req =
        new TableUpdateRequest.UpdateTableColumnPositionRequest(
            new String[] {"col1"}, TableChange.ColumnPosition.first());

    testAlterTable(tableId, req, expectedTable);
  }

  private DistributionDTO createMockDistributionDTO(String columnName, int bucketNum) {
    return DistributionDTO.builder()
        .withStrategy(Strategy.HASH)
        .withNumber(bucketNum)
        .withArgs(
            new FunctionArg[] {FieldReferenceDTO.builder().withColumnName(columnName).build()})
        .build();
  }

  private SortOrderDTO[] createMockSortOrderDTO(String columnName, SortDirection direction) {
    return new SortOrderDTO[] {
      SortOrderDTO.builder()
          .withDirection(direction)
          .withNullOrder(direction.defaultNullOrdering())
          .withSortTerm(
              FieldReferenceDTO.builder().withFieldName(new String[] {columnName}).build())
          .build()
    };
  }

  @Test
  public void testDeleteTableColumn() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    ColumnDTO[] columns =
        new ColumnDTO[] {createMockColumn("col2", Types.StringType.get(), "comment2")};

    DistributionDTO distributionDTO = createMockDistributionDTO("col2", 10);
    SortOrderDTO[] sortOrderDTOs = createMockSortOrderDTO("col2", DESCENDING);
    TableDTO expectedTable =
        createMockTable(
            "table1",
            columns,
            "comment",
            Collections.emptyMap(),
            EMPTY_PARTITIONING,
            distributionDTO,
            sortOrderDTOs);
    TableUpdateRequest.DeleteTableColumnRequest req =
        new TableUpdateRequest.DeleteTableColumnRequest(new String[] {"col1"}, true);

    testAlterTable(tableId, req, expectedTable);
  }

  @Test
  public void testDropTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath =
        withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace) + "/" + tableId.name());
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);

    Assertions.assertTrue(catalog.asTableCatalog().dropTable(tableId));

    // return false
    resp = new DropResponse(false);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);
    Assertions.assertFalse(catalog.asTableCatalog().dropTable(tableId));

    // Test with exception
    ErrorResponse errorResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, tablePath, null, errorResp, SC_INTERNAL_SERVER_ERROR);

    Throwable excep =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asTableCatalog().dropTable(tableId));
    Assertions.assertTrue(excep.getMessage().contains("internal error"));
  }

  @Test
  public void testPurgeTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath =
        withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace) + "/" + tableId.name());
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);

    Assertions.assertTrue(catalog.asTableCatalog().purgeTable(tableId));

    // return false
    resp = new DropResponse(false);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);
    Assertions.assertFalse(catalog.asTableCatalog().purgeTable(tableId));

    // Test with exception
    ErrorResponse errorResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, tablePath, null, errorResp, SC_INTERNAL_SERVER_ERROR);

    Throwable exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asTableCatalog().purgeTable(tableId));
    Assertions.assertTrue(exception.getMessage().contains("internal error"));
  }

  @Test
  public void testPurgeExternalTable() throws JsonProcessingException {
    NameIdentifier tableId = NameIdentifier.of("schema1", "table1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, tableId.namespace().level(0));
    String tablePath =
        withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace) + "/" + tableId.name());
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);

    Assertions.assertTrue(catalog.asTableCatalog().purgeTable(tableId));

    // return false
    resp = new DropResponse(false);
    buildMockResource(Method.DELETE, tablePath, null, resp, SC_OK);
    Assertions.assertFalse(catalog.asTableCatalog().purgeTable(tableId));

    // Test with exception
    ErrorResponse errorResp = ErrorResponse.unsupportedOperation("Unsupported operation");
    buildMockResource(Method.DELETE, tablePath, null, errorResp, SC_METHOD_NOT_ALLOWED);

    TableCatalog tableCatalog = catalog.asTableCatalog();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> tableCatalog.purgeTable(tableId),
        "Unsupported operation");
  }

  private void testAlterTable(NameIdentifier ident, TableUpdateRequest req, TableDTO updatedTable)
      throws JsonProcessingException {
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, ident.namespace().level(0));
    String tablePath =
        withSlash(RelationalCatalog.formatTableRequestPath(fullNamespace) + "/" + ident.name());
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(ImmutableList.of(req));
    TableResponse resp = new TableResponse(updatedTable);
    buildMockResource(Method.PUT, tablePath, updatesRequest, resp, SC_OK);

    Table alteredTable = catalog.asTableCatalog().alterTable(ident, req.tableChange());
    Assertions.assertEquals(updatedTable.name(), alteredTable.name());
    Assertions.assertEquals(updatedTable.comment(), alteredTable.comment());
    Assertions.assertEquals(updatedTable.properties(), alteredTable.properties());

    Assertions.assertEquals(updatedTable.columns().length, alteredTable.columns().length);
    for (int i = 0; i < updatedTable.columns().length; i++) {
      Assertions.assertEquals(updatedTable.columns()[i].name(), alteredTable.columns()[i].name());
      Assertions.assertEquals(
          updatedTable.columns()[i].dataType(), alteredTable.columns()[i].dataType());
      Assertions.assertEquals(
          updatedTable.columns()[i].comment(), alteredTable.columns()[i].comment());
      Assertions.assertEquals(
          updatedTable.columns()[i].nullable(), alteredTable.columns()[i].nullable());
    }

    Assertions.assertArrayEquals(updatedTable.partitioning(), alteredTable.partitioning());
  }

  private void testAlterSchema(
      NameIdentifier ident, SchemaUpdateRequest req, SchemaDTO updatedSchema)
      throws JsonProcessingException {
    String schemaPath =
        withSlash(
            RelationalCatalog.formatSchemaRequestPath(ident.namespace()) + "/" + ident.name());
    SchemaUpdatesRequest updatesReq = new SchemaUpdatesRequest(ImmutableList.of(req));
    SchemaResponse resp = new SchemaResponse(updatedSchema);
    buildMockResource(Method.PUT, schemaPath, updatesReq, resp, SC_OK);

    Schema alteredSchema = catalog.asSchemas().alterSchema(ident.name(), req.schemaChange());
    Assertions.assertEquals(updatedSchema.name(), alteredSchema.name());
    Assertions.assertEquals(updatedSchema.comment(), alteredSchema.comment());
    Assertions.assertEquals(updatedSchema.properties(), alteredSchema.properties());
  }

  protected static SchemaDTO createMockSchema(
      String name, String comment, Map<String, String> props) {
    return SchemaDTO.builder()
        .withName(name)
        .withComment(comment)
        .withProperties(props)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  protected static ColumnDTO createMockColumn(String name, Type type, String comment) {
    return createMockColumn(name, type, comment, true);
  }

  private static ColumnDTO createMockColumn(
      String name, Type type, String comment, boolean nullable) {
    return new ColumnDTO.Builder<>()
        .withName(name)
        .withDataType(type)
        .withComment(comment)
        .withNullable(nullable)
        .build();
  }

  private static ColumnDTO createMockColumn(
      String name, Type type, String comment, boolean nullable, LiteralDTO defaultValue) {
    return new ColumnDTO.Builder<>()
        .withName(name)
        .withDataType(type)
        .withComment(comment)
        .withNullable(nullable)
        .withDefaultValue(defaultValue)
        .build();
  }

  protected static TableDTO createMockTable(
      String name,
      ColumnDTO[] columns,
      String comment,
      Map<String, String> properties,
      Partitioning[] partitioning,
      DistributionDTO distributionDTO,
      SortOrderDTO[] sortOrderDTOs) {
    return createMockTable(
        name,
        columns,
        comment,
        properties,
        partitioning,
        distributionDTO,
        sortOrderDTOs,
        IndexDTO.EMPTY_INDEXES);
  }

  private static TableDTO createMockTable(
      String name,
      ColumnDTO[] columns,
      String comment,
      Map<String, String> properties,
      Partitioning[] partitioning,
      DistributionDTO distributionDTO,
      SortOrderDTO[] sortOrderDTOs,
      IndexDTO[] indexDTOs) {
    return TableDTO.builder()
        .withName(name)
        .withColumns(columns)
        .withComment(comment)
        .withProperties(properties)
        .withDistribution(distributionDTO)
        .withSortOrders(sortOrderDTOs)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .withPartitioning(partitioning)
        .withIndex(indexDTOs)
        .build();
  }
}
