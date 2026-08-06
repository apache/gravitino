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

import static org.apache.gravitino.dto.util.DTOConverters.fromDTOs;
import static org.apache.gravitino.dto.util.DTOConverters.toDTO;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_CONFLICT;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.http.HttpStatus.SC_NOT_IMPLEMENTED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import java.util.Collections;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.DistributionDTO;
import org.apache.gravitino.dto.rel.SortOrderDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.dto.requests.AddPartitionsRequest;
import org.apache.gravitino.dto.requests.SchemaCreateRequest;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.PartitionListResponse;
import org.apache.gravitino.dto.responses.PartitionNameListResponse;
import org.apache.gravitino.dto.responses.PartitionResponse;
import org.apache.gravitino.dto.responses.SchemaResponse;
import org.apache.gravitino.dto.responses.TableResponse;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRelationalTable extends TestRelationalCatalog {

  private static Table partitionedTable;
  private static final String schemaName = "testSchema";
  private static final String partitionedTableName = "testPartitionedTable";

  @BeforeAll
  public static void setUp() throws Exception {
    TestRelationalCatalog.setUp();

    // setup schema
    String schemaPath =
        withSlash(
            RelationalCatalog.formatSchemaRequestPath(Namespace.of(metalakeName, catalogName)));
    SchemaDTO mockedSchema = createMockSchema(schemaName, "comment", Collections.emptyMap());

    SchemaCreateRequest req =
        new SchemaCreateRequest(schemaName, "comment", Collections.emptyMap());
    SchemaResponse resp = new SchemaResponse(mockedSchema);
    buildMockResource(Method.POST, schemaPath, req, resp, SC_OK);

    catalog.asSchemas().createSchema(schemaName, "comment", Collections.emptyMap());

    // setup partitioned table
    NameIdentifier tableId = NameIdentifier.of(schemaName, partitionedTableName);
    String tablePath =
        withSlash(
            RelationalCatalog.formatTableRequestPath(
                Namespace.of(metalakeName, catalogName, schemaName)));

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
                fromDTOs(columns),
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

    SupportsPartitions partitions = partitionedTable.supportPartitions();
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> partitions.listPartitionNames());
    Assertions.assertEquals("table does not support partition operations", exception.getMessage());
  }

  @Test
  public void testListPartitions() throws JsonProcessingException {
    String partitionName = "p1";
    RangePartitionDTO partition =
        RangePartitionDTO.builder()
            .withName(partitionName)
            .withLower(
                LiteralDTO.builder().withDataType(Types.IntegerType.get()).withValue("1").build())
            .withUpper(
                LiteralDTO.builder().withDataType(Types.IntegerType.get()).withValue("10").build())
            .build();
    String partitionPath =
        withSlash(((RelationalTable) partitionedTable).getPartitionRequestPath());
    PartitionListResponse resp = new PartitionListResponse(new PartitionDTO[] {partition});

    buildMockResource(Method.GET, partitionPath, null, resp, SC_OK);

    Partition[] partitions = partitionedTable.supportPartitions().listPartitions();
    Assertions.assertEquals(1, partitions.length);
    Assertions.assertTrue(partitions[0] instanceof RangePartition);
    Assertions.assertEquals(partition, partitions[0]);

    // test throws exception
    ErrorResponse errorResp =
        ErrorResponse.unsupportedOperation("table does not support partition operations");
    buildMockResource(Method.GET, partitionPath, null, errorResp, SC_NOT_IMPLEMENTED);

    SupportsPartitions supportPartitions = partitionedTable.supportPartitions();
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> supportPartitions.listPartitions());
    Assertions.assertEquals("table does not support partition operations", exception.getMessage());
  }

  @Test
  public void testGetPartition() throws JsonProcessingException {
    String partitionName = "p1";
    RangePartitionDTO partition =
        RangePartitionDTO.builder()
            .withName(partitionName)
            .withLower(
                LiteralDTO.builder().withDataType(Types.IntegerType.get()).withValue("1").build())
            .withUpper(
                LiteralDTO.builder().withDataType(Types.IntegerType.get()).withValue("10").build())
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

    SupportsPartitions partitions = partitionedTable.supportPartitions();
    NoSuchPartitionException exception =
        Assertions.assertThrows(
            NoSuchPartitionException.class, () -> partitions.getPartition(partitionName));
    Assertions.assertEquals("partition not found", exception.getMessage());
  }

  @Test
  public void testAddPartition() throws JsonProcessingException {
    String partitionName = "p1";
    Literal<?>[] listValue1 = {Literals.integerLiteral(1)};
    Literal<?>[] listValue2 = {Literals.integerLiteral(3)};
    Literal<?>[] listValue3 = {Literals.integerLiteral(5)};
    Literal<?>[][] listValues = {listValue1, listValue2, listValue3};
    Partition partition = Partitions.list(partitionName, listValues, Maps.newHashMap());

    RelationalTable table = (RelationalTable) partitionedTable;
    String partitionPath = withSlash(table.getPartitionRequestPath());
    AddPartitionsRequest req = new AddPartitionsRequest(new PartitionDTO[] {toDTO(partition)});
    PartitionListResponse resp = new PartitionListResponse(new PartitionDTO[] {toDTO(partition)});
    buildMockResource(Method.POST, partitionPath, req, resp, SC_OK);

    Partition addedPartition = partitionedTable.supportPartitions().addPartition(partition);
    Assertions.assertEquals(toDTO(partition), addedPartition);

    // test throws exception
    ErrorResponse errorResp =
        ErrorResponse.alreadyExists(
            PartitionAlreadyExistsException.class.getSimpleName(), "partition already exists");
    buildMockResource(Method.POST, partitionPath, req, errorResp, SC_CONFLICT);

    SupportsPartitions partitions = partitionedTable.supportPartitions();
    PartitionAlreadyExistsException exception =
        Assertions.assertThrows(
            PartitionAlreadyExistsException.class, () -> partitions.addPartition(partition));
    Assertions.assertEquals("partition already exists", exception.getMessage());
  }

  @Test
  public void testDropPartition() throws JsonProcessingException {
    String partitionName = "p1";

    RelationalTable table = (RelationalTable) partitionedTable;
    String partitionPath =
        withSlash(
            RelationalTable.formatPartitionRequestPath(
                table.getPartitionRequestPath(), partitionName));
    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, partitionPath, null, resp, SC_OK);
    Assertions.assertTrue(table.supportPartitions().dropPartition(partitionName));

    // test not exist exception
    DropResponse notExistResp = new DropResponse(false);
    buildMockResource(Method.DELETE, partitionPath, null, notExistResp, SC_OK);
    Assertions.assertFalse(table.supportPartitions().dropPartition(partitionName));
  }
}
