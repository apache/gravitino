package org.apache.gravitino.flink.connector.integration.test.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.flink.connector.integration.test.FlinkEnvIT;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.apache.gravitino.flink.connector.paimon.GravitinoPaimonCatalogFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FlinkPaimonCatalogIT extends FlinkEnvIT {

  private static final String DEFAULT_PAIMON_CATALOG =
      "test_flink_paimon_filesystem_schema_catalog";

  private static org.apache.gravitino.Catalog catalog;

  protected Catalog currentCatalog() {
    return catalog;
  }

  @BeforeAll
  static void setup() {
    initPaimonCatalog();
  }

  @AfterAll
  static void stop() {
    Preconditions.checkNotNull(metalake);
    metalake.dropCatalog(DEFAULT_PAIMON_CATALOG, true);
  }

  private static void initPaimonCatalog() {
    Preconditions.checkNotNull(metalake);
    catalog =
        metalake.createCatalog(
            DEFAULT_PAIMON_CATALOG,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            "lakehouse-paimon",
            null,
            ImmutableMap.of(
                GravitinoPaimonCatalogFactoryOptions.backendType.key(),
                "filesystem",
                "warehouse",
                "/tmp/gravitino/paimon"));
  }

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
          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(tableEnv.executeSql("USE " + schema), ResultKind.SUCCESS);

            catalog.asSchemas().schemaExists(schema);
            Schema loadedSchema = catalog.asSchemas().loadSchema(schema);
            Assertions.assertEquals(schema, loadedSchema.name());
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
            Arrays.sort(schemas);
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
}
