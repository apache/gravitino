/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_NOT_IMPLEMENTED;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitionDTO;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.TableCreateRequest;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.PartitionNameListResponse;
import com.datastrato.gravitino.dto.responses.PartitionResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.dto.responses.TableResponse;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.partitions.Partition;
import com.datastrato.gravitino.rel.partitions.RangePartition;
import com.datastrato.gravitino.rel.types.Types;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Collections;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRelationalTable extends TestRelationalCatalog {

  private static Schema schema;
  private static Table partitionedTable;
  private static final String schemaName = "testSchema";
  private static final String partitionedTableName = "testPartitionedTable";

  @BeforeAll
  public static void setUp() throws Exception {
    TestRelationalCatalog.setUp();

    // setup schema
    NameIdentifier schemaId = NameIdentifier.of(metalakeName, catalogName, schemaName);
    String schemaPath = withSlash(RelationalCatalog.formatSchemaRequestPath(schemaId.namespace()));
    SchemaDTO mockedSchema = createMockSchema(schemaName, "comment", Collections.emptyMap());

    SchemaCreateRequest req =
        new SchemaCreateRequest(schemaName, "comment", Collections.emptyMap());
    SchemaResponse resp = new SchemaResponse(mockedSchema);
    buildMockResource(Method.POST, schemaPath, req, resp, SC_OK);

    schema = catalog.asSchemas().createSchema(schemaId, "comment", Collections.emptyMap());

    // setup partitioned table
    NameIdentifier tableId =
        NameIdentifier.of(metalakeName, catalogName, schemaName, partitionedTableName);
    String tablePath = withSlash(RelationalCatalog.formatTableRequestPath(tableId.namespace()));

    ColumnDTO[] columns =
        new ColumnDTO[] {
          createMockColumn("city", Types.IntegerType.get(), "comment1"),
          createMockColumn("dt", Types.DateType.get(), "comment2")
        };
    Partitioning[] partitioning = {
      IdentityPartitioningDTO.of(columns[0].name()), DayPartitioningDTO.of(columns[1].name())
    };
    TableDTO mockedTable =
        createMockTable(
            tableId.name(),
            columns,
            "comment",
            Collections.emptyMap(),
            partitioning,
            DistributionDTO.NONE,
            SortOrderDTO.EMPTY_SORT);

    TableCreateRequest tableCreateRequest =
        new TableCreateRequest(
            tableId.name(),
            "comment",
            columns,
            Collections.emptyMap(),
            SortOrderDTO.EMPTY_SORT,
            DistributionDTO.NONE,
            partitioning,
            IndexDTO.EMPTY_INDEXES);
    TableResponse tableResponse = new TableResponse(mockedTable);
    buildMockResource(Method.POST, tablePath, tableCreateRequest, tableResponse, SC_OK);

    partitionedTable =
        catalog
            .asTableCatalog()
            .createTable(
                tableId,
                columns,
                "comment",
                Collections.emptyMap(),
                partitioning,
                DistributionDTO.NONE,
                SortOrderDTO.EMPTY_SORT);
  }

  @Test
  public void testListPartitionNames() throws JsonProcessingException {
    String[] names = {"p1", "p2"};
    String partitionPath =
        withSlash(((RelationalTable) partitionedTable).getPartitionRequestPath());
    PartitionNameListResponse resp = new PartitionNameListResponse(names);

    buildMockResource(Method.GET, partitionPath, null, resp, SC_OK);

    String[] partitionNames = partitionedTable.supportPartitions().listPartitionNames();
    Assertions.assertEquals(2, partitionNames.length);
    Assertions.assertEquals(names[0], partitionNames[0]);
    Assertions.assertEquals(names[1], partitionNames[1]);

    // test throws exception
    ErrorResponse errorResp =
        ErrorResponse.unsupportedOperation("table does not support partition operations");
    buildMockResource(Method.GET, partitionPath, null, errorResp, SC_NOT_IMPLEMENTED);

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> partitionedTable.supportPartitions().listPartitionNames());
    Assertions.assertEquals("table does not support partition operations", exception.getMessage());
  }

  @Test
  public void testGetPartition() throws JsonProcessingException {
    String partitionName = "p1";
    RangePartitionDTO partition =
        RangePartitionDTO.builder()
            .withName(partitionName)
            .withLower(
                new LiteralDTO.Builder()
                    .withDataType(Types.IntegerType.get())
                    .withValue("1")
                    .build())
            .withUpper(
                new LiteralDTO.Builder()
                    .withDataType(Types.IntegerType.get())
                    .withValue("10")
                    .build())
            .build();
    RelationalTable table = (RelationalTable) partitionedTable;
    String partitionPath =
        withSlash(
            RelationalTable.formatPartitionRequestPath(
                table.getPartitionRequestPath(), partitionName));
    PartitionResponse resp = new PartitionResponse(partition);
    buildMockResource(Method.GET, partitionPath, null, resp, SC_OK);

    Partition actualPartition = table.supportPartitions().getPartition(partitionName);
    Assertions.assertTrue(actualPartition instanceof RangePartition);
    Assertions.assertEquals(partition, actualPartition);

    // test throws exception
    ErrorResponse errorResp =
        ErrorResponse.notFound(
            NoSuchPartitionException.class.getSimpleName(), "partition not found");
    buildMockResource(Method.GET, partitionPath, null, errorResp, SC_NOT_FOUND);

    NoSuchPartitionException exception =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> table.supportPartitions().getPartition(partitionName));
    Assertions.assertEquals("partition not found", exception.getMessage());
  }
}
