/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_CATALOG_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_COMMENT;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.HIVE_SCHEMA_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.META_LAKE_NAME;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.initHiveCatalog;
import static com.datastrato.gravitino.catalog.hive.TestHiveTable.initHiveSchema;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.identity;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.catalog.hive.miniHMS.MiniHiveMetastoreService;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestHiveTableOperations extends MiniHiveMetastoreService {

  private static final NameIdentifier tableIdentifier =
      NameIdentifier.of(META_LAKE_NAME, HIVE_CATALOG_NAME, HIVE_SCHEMA_NAME, genRandomName());
  private static HiveCatalog hiveCatalog;
  private static HiveSchema hiveSchema;
  private static HiveTable hiveTable;

  @BeforeAll
  private static void setup() {
    hiveCatalog = initHiveCatalog();
    hiveSchema = initHiveSchema(hiveCatalog);
    hiveTable = createPartitionedTable();
  }

  private static HiveTable createPartitionedTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");

    HiveColumn col1 =
        new HiveColumn.Builder().withName("city").withType(Types.ByteType.get()).build();
    HiveColumn col2 =
        new HiveColumn.Builder().withName("dt").withType(Types.DateType.get()).build();
    Column[] columns = new Column[] {col1, col2};

    Transform[] partitioning = new Transform[] {identity(col2.name())};

    return (HiveTable)
        hiveCatalog
            .asTableCatalog()
            .createTable(tableIdentifier, columns, HIVE_COMMENT, properties, partitioning);
  }

  @Test
  public void testListPartitionNames() {
    String[] partitionNames = hiveTable.supportPartitions().listPartitionNames();
    // TODO: update following assertion after implementing addPartition
    Assertions.assertEquals(0, partitionNames.length);
  }

  @Test
  public void testGetPartition() {
    NoSuchPartitionException exception =
        Assertions.assertThrows(
            NoSuchPartitionException.class,
            () -> {
              hiveTable.supportPartitions().getPartition("does_not_exist_partition");
            });
    Assertions.assertEquals(
        "Hive partition does_not_exist_partition does not exist in Hive Metastore",
        exception.getMessage());
  }
}
