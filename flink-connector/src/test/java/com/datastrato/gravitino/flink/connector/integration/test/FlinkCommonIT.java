/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.integration.test;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public abstract class FlinkCommonIT extends FlinkEnvIT {

  protected abstract Catalog currentCatalog();

  @Test
  public void testCreateSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_create_schema";
          try {
            TableResult tableResult = sql("CREATE DATABASE IF NOT EXISTS %s", schema);
            TestUtils.assertTableResult(tableResult, ResultKind.SUCCESS);
            catalog.asSchemas().schemaExists(schema);
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testGetSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_get_schema";
          String comment = "test comment";
          String propertyKey = "key1";
          String propertyValue = "value1";
          String location = warehouse + "/" + schema;

          try {
            TestUtils.assertTableResult(
                sql(
                    "CREATE DATABASE IF NOT EXISTS %s COMMENT '%s' WITH ('%s'='%s', '%s'='%s')",
                    schema, comment, propertyKey, propertyValue, "location", location),
                ResultKind.SUCCESS);
            TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

            catalog.asSchemas().schemaExists(schema);
            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
            Assertions.assertEquals(comment, loadedSchema.comment());
            Assertions.assertEquals(2, loadedSchema.properties().size());
            Assertions.assertEquals(propertyValue, loadedSchema.properties().get(propertyKey));
            Assertions.assertEquals(
                location, loadedSchema.properties().get(HiveSchemaPropertiesMetadata.LOCATION));
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            Assertions.assertFalse(catalog.asSchemas().schemaExists(schema));
          }
        });
  }

  @Test
  public void testListSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          String schema = "test_list_schema";
          String schema2 = "test_list_schema2";
          String schema3 = "test_list_schema3";

          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema2), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema3), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("SHOW DATABASES"),
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of("default"),
                Row.of(schema),
                Row.of(schema2),
                Row.of(schema3));

            String[] schemas = catalog.asSchemas().listSchemas();
            Assertions.assertEquals(4, schemas.length);
            Assertions.assertEquals("default", schemas[0]);
            Assertions.assertEquals(schema, schemas[1]);
            Assertions.assertEquals(schema2, schemas[2]);
            Assertions.assertEquals(schema3, schemas[3]);
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
            catalog.asSchemas().dropSchema(schema2, true);
            catalog.asSchemas().dropSchema(schema3, true);
            Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          }
        });
  }

  @Test
  public void testAlterSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_alter_schema";
          try {
            TestUtils.assertTableResult(
                sql(
                    "CREATE DATABASE IF NOT EXISTS %s "
                        + "COMMENT 'test comment'"
                        + "WITH ('key1' = 'value1', 'key2'='value2')",
                    schema),
                ResultKind.SUCCESS);

            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
            Assertions.assertEquals("test comment", loadedSchema.comment());
            Assertions.assertEquals(3, loadedSchema.properties().size());
            Assertions.assertEquals("value1", loadedSchema.properties().get("key1"));
            Assertions.assertEquals("value2", loadedSchema.properties().get("key2"));
            Assertions.assertNotNull(loadedSchema.properties().get("location"));

            TestUtils.assertTableResult(
                sql("ALTER DATABASE %s SET ('key1'='new-value', 'key3'='value3')", schema),
                ResultKind.SUCCESS);
            Schema reloadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, reloadedSchema.name());
            Assertions.assertEquals("test comment", reloadedSchema.comment());
            Assertions.assertEquals(4, reloadedSchema.properties().size());
            Assertions.assertEquals("new-value", reloadedSchema.properties().get("key1"));
            Assertions.assertEquals("value3", reloadedSchema.properties().get("key3"));
          } finally {
            catalog.asSchemas().dropSchema(schema, true);
          }
        });
  }
}
