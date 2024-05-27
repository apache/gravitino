/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.integration.test.hive;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.flink.connector.integration.test.FlinkEnvIT;
import com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlinkHiveCatalogSchemaIT extends FlinkEnvIT {

  @Test
  public void testCreateSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    String schema = "test_create_schema";
    TableResult tableResult =
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema));
    TestUtils.assertTableResult(tableResult, ResultKind.SUCCESS);
    Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    catalog.asSchemas().schemaExists(schema);
    catalog.asSchemas().dropSchema(schema, true);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
  }

  @Test
  public void testGetSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    String schema = "test_get_schema";
    TestUtils.assertTableResult(
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema)),
        ResultKind.SUCCESS);
    TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

    Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    catalog.asSchemas().schemaExists(schema);
    Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
    Assertions.assertEquals(schema, loadedSchema.name());
    catalog.asSchemas().dropSchema(schema, true);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
  }

  @Test
  public void testListSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    String schema = "test_list_schema";
    TestUtils.assertTableResult(
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema)),
        ResultKind.SUCCESS);
    String schema2 = "test_list_schema2";
    TestUtils.assertTableResult(
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema2)),
        ResultKind.SUCCESS);
    String schema3 = "test_list_schema3";
    TestUtils.assertTableResult(
        tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS %s", schema3)),
        ResultKind.SUCCESS);
    TestUtils.assertTableResult(
        tableEnv.executeSql("SHOW DATABASES"),
        ResultKind.SUCCESS_WITH_CONTENT,
        Row.of(schema),
        Row.of(schema2),
        Row.of(schema3));

    Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    NameIdentifier[] nameIdentifiers = catalog.asSchemas().listSchemas();
    Assertions.assertEquals(3, nameIdentifiers.length);
    Assertions.assertEquals(schema, nameIdentifiers[0].name());
    Assertions.assertEquals(schema2, nameIdentifiers[1].name());
    Assertions.assertEquals(schema3, nameIdentifiers[2].name());

    catalog.asSchemas().dropSchema(schema, true);
    catalog.asSchemas().dropSchema(schema2, true);
    catalog.asSchemas().dropSchema(schema3, true);
    Assertions.assertEquals(0, catalog.asSchemas().listSchemas().length);
  }

  @Test
  public void testAlterSchema() {
    tableEnv.useCatalog(defaultHiveCatalog);
    String schema = "test_alter_schema";
    TestUtils.assertTableResult(
        tableEnv.executeSql(
            String.format(
                "CREATE DATABASE IF NOT EXISTS %s "
                    + "COMMENT 'test comment'"
                    + "WITH ('key1' = 'value1', 'key2'='value2')",
                schema)),
        ResultKind.SUCCESS);
    Catalog catalog = metalake.loadCatalog(defaultHiveCatalog);
    Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
    Assertions.assertEquals(schema, loadedSchema.name());
    Assertions.assertEquals("test comment", loadedSchema.comment());
    Assertions.assertEquals(2, loadedSchema.properties().size());
    Assertions.assertEquals("value1", loadedSchema.properties().get("key1"));
    Assertions.assertEquals("value2", loadedSchema.properties().get("key2"));

    TestUtils.assertTableResult(
        tableEnv.executeSql("ALTER DATABASE %s SET ('key1'='new-value', 'key3'='value3')"),
        ResultKind.SUCCESS);
    Schema reloadedSchema = catalog.asSchemas().loadSchema(schema);
    Assertions.assertEquals(schema, reloadedSchema.name());
    Assertions.assertEquals("test comment", reloadedSchema.comment());
    Assertions.assertEquals(2, reloadedSchema.properties().size());
    Assertions.assertEquals("new-value", reloadedSchema.properties().get("key1"));
    Assertions.assertEquals("value3", reloadedSchema.properties().get("key3"));
  }
}
