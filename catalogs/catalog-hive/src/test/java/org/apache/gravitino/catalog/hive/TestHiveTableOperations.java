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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.TestHiveTable.HIVE_CATALOG_NAME;
import static org.apache.gravitino.catalog.hive.TestHiveTable.HIVE_COMMENT;
import static org.apache.gravitino.catalog.hive.TestHiveTable.HIVE_SCHEMA_NAME;
import static org.apache.gravitino.catalog.hive.TestHiveTable.META_LAKE_NAME;
import static org.apache.gravitino.catalog.hive.TestHiveTable.initHiveCatalog;
import static org.apache.gravitino.catalog.hive.TestHiveTable.initHiveSchema;
import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;

import com.google.common.collect.Maps;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.hive.hms.MiniHiveMetastoreService;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHiveTableOperations extends MiniHiveMetastoreService {

  private static final NameIdentifier tableIdentifier =
      NameIdentifier.of(META_LAKE_NAME, HIVE_CATALOG_NAME, HIVE_SCHEMA_NAME, genRandomName());
  private static HiveCatalog hiveCatalog;
  private static HiveCatalogOperations hiveCatalogOperations;
  private static HiveTable hiveTable;
  private static Column[] columns;
  private static Partition existingPartition;

  @BeforeAll
  public static void setup() {
    hiveCatalog = initHiveCatalog();
    hiveCatalogOperations = (HiveCatalogOperations) hiveCatalog.ops();
    initHiveSchema(hiveCatalogOperations);
    hiveTable = createPartitionedTable();

    // add partition: city=0/dt=2020-01-01
    String[] fieldCity = new String[] {columns[1].name()};
    Literal<?> valueCity = Literals.byteLiteral((byte) 0);
    String[] fieldDt = new String[] {columns[2].name()};
    Literal<?> valueDt = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition =
        Partitions.identity(
            new String[][] {fieldCity, fieldDt}, new Literal<?>[] {valueCity, valueDt});

    existingPartition = hiveTable.supportPartitions().addPartition(partition);
  }

  private static HiveTable createPartitionedTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col0 =
        HiveColumn.builder().withName("name").withType(Types.StringType.get()).build();
    HiveColumn col1 = HiveColumn.builder().withName("city").withType(Types.ByteType.get()).build();
    HiveColumn col2 = HiveColumn.builder().withName("dt").withType(Types.DateType.get()).build();

    columns = new Column[] {col0, col1, col2};

    Transform[] partitioning = new Transform[] {identity(col1.name()), identity(col2.name())};

    return (HiveTable)
        hiveCatalogOperations.createTable(
            tableIdentifier, columns, HIVE_COMMENT, properties, partitioning);
  }

  @Test
  public void testListPartitionNames() {
    String[] partitionNames = hiveTable.supportPartitions().listPartitionNames();
    // there maybe other partitions in the list, so we only check the added partition
    Assertions.assertTrue(
        partitionNames.length > 0
            && Arrays.asList(partitionNames).contains(existingPartition.name()));
  }

  @Test
  public void testListPartitions() {
    Partition[] partitions = hiveTable.supportPartitions().listPartitions();
    // there maybe other partitions in the list, so we only check the added partition
    Assertions.assertTrue(
        partitions.length > 0 && Arrays.asList(partitions).contains(existingPartition));
  }

  @Test
  public void testGetPartition() {
    SupportsPartitions partitions = hiveTable.supportPartitions();
    Partition partition = partitions.getPartition(existingPartition.name());
    Assertions.assertEquals(existingPartition, partition);

    NoSuchPartitionException exception =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              partitions.getPartition("does_not_exist_partition");
            });
    Assertions.assertEquals(
        "Hive partition does_not_exist_partition does not exist in Hive Metastore",
        exception.getMessage());
  }

  @Test
  public void testAddPartition() {
    // add partition: city=1/dt=2020-01-01
    String[] fieldCity = new String[] {columns[1].name()};
    Literal<?> valueCity = Literals.byteLiteral((byte) 1);
    String[] fieldDt = new String[] {columns[2].name()};
    Literal<?> valueDt = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition =
        Partitions.identity(
            new String[][] {fieldCity, fieldDt}, new Literal<?>[] {valueCity, valueDt});

    SupportsPartitions partitions = hiveTable.supportPartitions();
    Partition addedPartition = partitions.addPartition(partition);
    Partition gotPartition = partitions.getPartition(addedPartition.name());
    Assertions.assertEquals(addedPartition, gotPartition);

    // test exception
    String[] field1 = new String[] {columns[0].name()};
    Literal<?> value1 = Literals.byteLiteral((byte) 1);
    Partition partition1 = Partitions.identity(new String[][] {field1}, new Literal<?>[] {value1});
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> partitions.addPartition(partition1));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(
                "Hive partition field names must be the same as table partitioning field names"),
        exception.getMessage());

    String[] field2 = new String[] {columns[1].name()};
    Literal<?> value2 = Literals.byteLiteral((byte) 1);
    Partition partition2 =
        Partitions.identity(new String[][] {field1, field2}, new Literal<?>[] {value1, value2});
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> partitions.addPartition(partition2));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Hive partition field name must be in table partitioning field names"),
        exception.getMessage());
  }

  @Test
  public void testDropPartition() {
    // add partition: city=2/dt=2020-01-01
    String partitionName1 = "city=2/dt=2020-01-01";
    String[] fieldCity1 = new String[] {columns[1].name()};
    Literal<?> valueCity1 = Literals.byteLiteral((byte) 2);
    String[] fieldDt1 = new String[] {columns[2].name()};
    Literal<?> valueDt1 = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition1 =
        Partitions.identity(
            new String[][] {fieldCity1, fieldDt1}, new Literal<?>[] {valueCity1, valueDt1});
    hiveTable.supportPartitions().addPartition(partition1);

    // add partition: city=3/dt=2020-01-01
    String partitionName2 = "city=3/dt=2020-01-01";
    String[] fieldCity2 = new String[] {columns[1].name()};
    Literal<?> valueCity2 = Literals.byteLiteral((byte) 3);
    String[] fieldDt2 = new String[] {columns[2].name()};
    Literal<?> valueDt2 = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition2 =
        Partitions.identity(
            new String[][] {fieldCity2, fieldDt2}, new Literal<?>[] {valueCity2, valueDt2});
    hiveTable.supportPartitions().addPartition(partition2);

    // add partition: city=3/dt=2020-01-02
    String partitionName3 = "city=3/dt=2020-01-02";
    String[] fieldCity3 = new String[] {columns[1].name()};
    Literal<?> valueCity3 = Literals.byteLiteral((byte) 3);
    String[] fieldDt3 = new String[] {columns[2].name()};
    Literal<?> valueDt3 = Literals.dateLiteral(LocalDate.parse("2020-01-02"));
    Partition partition3 =
        Partitions.identity(
            new String[][] {fieldCity3, fieldDt3}, new Literal<?>[] {valueCity3, valueDt3});
    hiveTable.supportPartitions().addPartition(partition3);

    // add partition: city=4/dt=2020-01-01
    String partitionName4 = "city=4/dt=2020-01-01";
    String[] fieldCity4 = new String[] {columns[1].name()};
    Literal<?> valueCity4 = Literals.byteLiteral((byte) 4);
    String[] fieldDt4 = new String[] {columns[2].name()};
    Literal<?> valueDt4 = Literals.dateLiteral(LocalDate.parse("2020-01-01"));
    Partition partition4 =
        Partitions.identity(
            new String[][] {fieldCity4, fieldDt4}, new Literal<?>[] {valueCity4, valueDt4});
    hiveTable.supportPartitions().addPartition(partition4);

    // add partition: city=4/dt=2020-01-02
    String partitionName5 = "city=4/dt=2020-01-02";
    String[] fieldCity5 = new String[] {columns[1].name()};
    Literal<?> valueCity5 = Literals.byteLiteral((byte) 4);
    String[] fieldDt5 = new String[] {columns[2].name()};
    Literal<?> valueDt5 = Literals.dateLiteral(LocalDate.parse("2020-01-02"));
    Partition partition5 =
        Partitions.identity(
            new String[][] {fieldCity5, fieldDt5}, new Literal<?>[] {valueCity5, valueDt5});
    hiveTable.supportPartitions().addPartition(partition5);

    // test drop one partition: city=2/dt=2020-01-01
    hiveTable.supportPartitions().dropPartition(partitionName1);
    NoSuchPartitionException exception1 =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              hiveTable.supportPartitions().getPartition(partitionName1);
            });
    Assertions.assertEquals(
        String.format("Hive partition %s does not exist in Hive Metastore", partitionName1),
        exception1.getMessage());

    // test drop cascade partitions: city=3
    hiveTable.supportPartitions().dropPartition("city=3");
    NoSuchPartitionException exception2 =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              hiveTable.supportPartitions().getPartition(partitionName2);
            });
    Assertions.assertEquals(
        String.format("Hive partition %s does not exist in Hive Metastore", partitionName2),
        exception2.getMessage());
    NoSuchPartitionException exception3 =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              hiveTable.supportPartitions().getPartition(partitionName3);
            });
    Assertions.assertEquals(
        String.format("Hive partition %s does not exist in Hive Metastore", partitionName3),
        exception3.getMessage());

    // test drop one partition: city=4/dt=2020-01-01
    // check city=4/dt=2020-01-02 is still exist
    hiveTable.supportPartitions().dropPartition(partitionName4);
    NoSuchPartitionException exception4 =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              hiveTable.supportPartitions().getPartition(partitionName4);
            });
    Assertions.assertEquals(
        String.format("Hive partition %s does not exist in Hive Metastore", partitionName4),
        exception4.getMessage());
    Assertions.assertTrue(hiveTable.supportPartitions().partitionExists(partitionName5));

    // test not exist partition
    IllegalArgumentException exception5 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              hiveTable.supportPartitions().dropPartition("does_not_exist_partition");
            });
    Assertions.assertEquals(
        "Error partition format: does_not_exist_partition", exception5.getMessage());

    Assertions.assertFalse(
        hiveTable.supportPartitions().dropPartition("city=not_exist"),
        "partition should be non-existent");
  }

  @Test
  public void testPurgePartition() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          hiveTable.supportPartitions().purgePartition("city=1");
        });
  }
}
