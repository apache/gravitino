/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.hive.CombineHiveCatalog;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.memory.CombineInMemoryCatalog;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.memory.InMemoryCatalog;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCombineCatalog {

  private CombineCatalog combineCatalog;
  private InMemoryCatalog primaryCatalog;
  private CombineInMemoryCatalog secondaryCatalog;

  private static final Types.StructType testTableStruct =
      Types.StructType.of(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "b", Types.BooleanType.get()),
          optional(4, "i", Types.IntegerType.get()));

  private static final Types.StructType testTableStruct2 =
      Types.StructType.of(
          required(1, "b2", Types.BooleanType.get()), optional(2, "i2", Types.IntegerType.get()));

  @BeforeEach
  void init() {
    primaryCatalog = new InMemoryCatalog();
    primaryCatalog.initialize("primary", ImmutableMap.of());
    // It's a little tricky, make primary catalog and secondary catalog share same
    // FileIO, so the table metadata created by the primary catalog could be loaded by the secondary
    // catalog
    FileIO fileIO = primaryCatalog.io();
    secondaryCatalog = new CombineInMemoryCatalog(fileIO);
    secondaryCatalog.initialize("secondary", ImmutableMap.of());
    combineCatalog = new CombineCatalog(primaryCatalog, secondaryCatalog);
    combineCatalog.createNamespace(Namespace.of("t_ns"));
  }

  @Test
  void testCombineHiveCatalog() {
    CombineHiveCatalog hiveCatalog = new CombineHiveCatalog();
    hiveCatalog.setConf(new HdfsConfiguration());
    hiveCatalog.initialize("test", ImmutableMap.of());
    hiveCatalog.newTableOps(TableIdentifier.of("ns", "table"));
  }

  @Test
  void testCreateTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("t_ns", "test");
    Schema schema = new Schema(testTableStruct.fields());
    combineCatalog.createTable(tableIdentifier, schema);

    Table table = combineCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema.columns(), table.schema().columns());

    table = primaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema.columns(), table.schema().columns());
    String primaryTableLocation = ((BaseTable) table).operations().current().metadataFileLocation();

    table = secondaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema.columns(), table.schema().columns());
    String secondaryTableLocation =
        ((BaseTable) table).operations().current().metadataFileLocation();
    System.out.println(primaryTableLocation + "," + secondaryTableLocation);
    Assertions.assertEquals(primaryTableLocation, secondaryTableLocation);

    /*
    // clear table from primary catalog
    primaryCatalog.dropTable(tableIdentifier);

    // if the secondary catalog has a table already, expected to overwrite it
    Schema schema2 = new Schema(testTableStruct2.fields());
    combineCatalog.createTable(tableIdentifier, schema2);

    table = combineCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema2.columns(), table.schema().columns());

    table = primaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema2.columns(), table.schema().columns());

    table = secondaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(schema2.columns(), table.schema().columns());
     */
  }

  @Test
  void testDropTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("t_ns", "t_drop");
    Schema schema = new Schema(testTableStruct.fields());
    combineCatalog.createTable(tableIdentifier, schema);
    Assertions.assertDoesNotThrow(
        () -> {
          combineCatalog.loadTable(tableIdentifier);
          primaryCatalog.loadTable(tableIdentifier);
          secondaryCatalog.loadTable(tableIdentifier);
        });

    combineCatalog.dropTable(tableIdentifier);

    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> combineCatalog.loadTable(tableIdentifier));
    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> primaryCatalog.loadTable(tableIdentifier));

    // todo: secondary catalog drop table failed
    Assertions.assertThrowsExactly(
        NotFoundException.class, () -> secondaryCatalog.loadTable(tableIdentifier));

    // if secondary catalog doesn't have the table
    TableIdentifier tableIdentifier2 = TableIdentifier.of("t_ns", "t_drop2");
    primaryCatalog.createTable(tableIdentifier2, schema);
    combineCatalog.dropTable(tableIdentifier2);

    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> combineCatalog.loadTable(tableIdentifier2));
    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> primaryCatalog.loadTable(tableIdentifier2));
    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> secondaryCatalog.loadTable(tableIdentifier2));
  }

  @Test
  void testLoadTable() {
    TableIdentifier tableIdentifier2 = TableIdentifier.of("t_ns", "t_load2");
    Schema schema2 = new Schema(testTableStruct2.fields());
    primaryCatalog.createTable(tableIdentifier2, schema2);
    Table table = combineCatalog.loadTable(tableIdentifier2);
    Assertions.assertEquals(schema2.columns(), table.schema().columns());
  }

  @Test
  void testListTable() {
    List<TableIdentifier> tables = combineCatalog.listTables(Namespace.of("t_ns"));
    Assertions.assertEquals(0, tables.size());

    TableIdentifier tableIdentifier = TableIdentifier.of("t_ns", "t_list");
    Schema schema = new Schema(testTableStruct.fields());
    primaryCatalog.createTable(tableIdentifier, schema);
    TableIdentifier tableIdentifier2 = TableIdentifier.of("t_ns", "t_list2");
    combineCatalog.createTable(tableIdentifier2, schema);

    tables = combineCatalog.listTables(Namespace.of("t_ns"));
    Assertions.assertEquals(2, tables.size());
    Assertions.assertEquals(TableIdentifier.of("t_ns", "t_list"), tables.get(0));
    Assertions.assertEquals(TableIdentifier.of("t_ns", "t_list2"), tables.get(1));
  }

  @Test
  void testAlterTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("t_ns", "t_alter");
    Schema schema = new Schema(testTableStruct.fields());
    combineCatalog.createTable(tableIdentifier, schema);

    Table table = combineCatalog.loadTable(tableIdentifier);
    UpdateSchema updateSchema = table.updateSchema().addColumn("add", Types.LongType.get());
    updateSchema.commit();

    List<NestedField> newFields = new ArrayList<>(schema.columns());
    newFields.add(NestedField.optional(5, "add", Types.LongType.get()));
    table = combineCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(newFields, table.schema().columns());

    table = primaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(newFields, table.schema().columns());

    table = secondaryCatalog.loadTable(tableIdentifier);
    Assertions.assertEquals(newFields, table.schema().columns());

    // if secondary catalog doesn't have the table
    TableIdentifier tableIdentifier2 = TableIdentifier.of("t_ns", "t_alter2");
    Schema schema2 = new Schema(testTableStruct2.fields());
    primaryCatalog.createTable(tableIdentifier2, schema2);

    Assertions.assertThrowsExactly(
        NoSuchTableException.class, () -> secondaryCatalog.loadTable(tableIdentifier2));

    table = combineCatalog.loadTable(tableIdentifier2);
    updateSchema = table.updateSchema().addColumn("add", Types.LongType.get());
    updateSchema.commit();

    newFields = new ArrayList<>(schema2.columns());
    newFields.add(NestedField.optional(3, "add", Types.LongType.get()));
    table = combineCatalog.loadTable(tableIdentifier2);
    Assertions.assertEquals(newFields, table.schema().columns());

    table = primaryCatalog.loadTable(tableIdentifier2);
    Assertions.assertEquals(newFields, table.schema().columns());

    table = secondaryCatalog.loadTable(tableIdentifier2);
    Assertions.assertEquals(newFields, table.schema().columns());
  }
}
