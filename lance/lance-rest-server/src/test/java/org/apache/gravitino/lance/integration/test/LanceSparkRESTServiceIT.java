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
package org.apache.gravitino.lance.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.rel.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LanceSparkRESTServiceIT extends BaseIT {

  private static final String LANCE_SPARK_CATALOG = "lance";
  private static final String LANCE_SPARK_CATALOG_CLASS =
      "org.lance.spark.LanceNamespaceSparkCatalog";
  private static final String LANCE_SPARK_BUNDLE_JAR_PATH_PROPERTY =
      "gravitino.lance.spark.bundle.jar";
  private static final String JAVA_MODULE_OPEN_OPTIONS =
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED";
  private static final String CATALOG_NAME = GravitinoITUtils.genRandomName("lance_spark_catalog");
  private static final String TABLE_COLUMNS = "(id INT, score FLOAT)";
  private static final String LANCE_CLASS_PREFIX = "org.lance.";
  private static final String LANCE_NATIVE_LOADER_CLASS_PREFIX = "io.questdb.jar.jni.";
  private static final String LANCE_NATIVE_RESOURCE_PREFIX = "nativelib/";

  private final Set<String> createdSchemas = new LinkedHashSet<>();

  private SparkSession sparkSession;
  private URLClassLoader sparkClientClassLoader;
  private GravitinoMetalake metalake;
  private Catalog catalog;
  private Path tempDir;

  @Override
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreLanceAuxRestService = false;
    super.startIntegrationTest();

    this.metalake = createMetalake(getLanceRESTServerMetalakeName());
    this.tempDir = Files.createTempDirectory("lance_spark_rest_service_it_");
    this.catalog = createCatalog(CATALOG_NAME);
    this.sparkSession = createSparkSession();
  }

  @AfterAll
  public void clean() throws Exception {
    Exception failure = null;

    try {
      stopSparkSession();
    } catch (Exception e) {
      failure = appendFailure(failure, e);
    }

    try {
      stopSparkClientClassLoader();
    } catch (Exception e) {
      failure = appendFailure(failure, e);
    }

    try {
      if (client != null) {
        client.dropMetalake(getLanceRESTServerMetalakeName(), true);
      }
    } catch (Exception e) {
      failure = appendFailure(failure, e);
    }

    try {
      if (tempDir != null) {
        FileUtils.deleteDirectory(tempDir.toFile());
      }
    } catch (Exception e) {
      failure = appendFailure(failure, e);
    }

    try {
      super.stopIntegrationTest();
    } catch (Exception e) {
      failure = appendFailure(failure, e);
    }

    if (failure != null) {
      throw failure;
    }
  }

  @AfterEach
  public void cleanupSchemas() {
    for (String schemaName : createdSchemas) {
      dropSchema(schemaName);
    }
    createdSchemas.clear();
  }

  @Test
  public void testCreateAndListNamespacesViaSpark() {
    String schemaName = createSchema("spark_ns_list");

    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));
    Assertions.assertTrue(listNamespaces().contains(schemaName));
  }

  @Test
  public void testCreateNamespaceWithPropertiesViaSpark() {
    String schemaName = newSchemaName("spark_ns_props");
    sql("CREATE DATABASE %s WITH DBPROPERTIES ('team'='analytics', 'env'='it')", schemaName);
    createdSchemas.add(schemaName);

    Schema schema = catalog.asSchemas().loadSchema(schemaName);
    Map<String, String> schemaProperties = schema.properties();

    Assertions.assertNotNull(schemaProperties);
    Assertions.assertEquals("analytics", schemaProperties.get("team"));
    Assertions.assertEquals("it", schemaProperties.get("env"));
  }

  @Test
  public void testCreateAndDescribeTableViaSpark() {
    String schemaName = createSchema("spark_tbl_desc");
    String tableName = newTableName("orders");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

    createLanceTable(schemaName, tableName);

    Assertions.assertTrue(catalog.asTableCatalog().tableExists(tableIdentifier));
    List<Row> tables = sql("SHOW TABLES IN %s", schemaName);
    Assertions.assertTrue(
        hasTable(tables, schemaName, tableName),
        String.format(
            "Expected table %s in schema %s, SHOW TABLES result: %s",
            tableName, schemaName, tables));

    Table table = catalog.asTableCatalog().loadTable(tableIdentifier);
    assertTableLocationAndFormat(table, schemaName, tableName);
  }

  @Test
  public void testInsertAndSelectMultipleBatchesViaSpark() {
    String schemaName = createSchema("spark_dml");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);
    sql(
        "INSERT INTO %s.%s VALUES (1, CAST(1.1 AS FLOAT)), (2, CAST(2.2 AS FLOAT))",
        schemaName, tableName);
    sql(
        "INSERT INTO %s.%s VALUES (3, CAST(3.3 AS FLOAT)), (4, CAST(4.4 AS FLOAT))",
        schemaName, tableName);

    List<Row> rows = sql("SELECT id, score FROM %s.%s ORDER BY id", schemaName, tableName);
    Assertions.assertEquals(4, rows.size());
    assertRow(rows.get(0), 1, 1.1f);
    assertRow(rows.get(1), 2, 2.2f);
    assertRow(rows.get(2), 3, 3.3f);
    assertRow(rows.get(3), 4, 4.4f);
  }

  @Test
  public void testSelectFromEmptyTableViaSpark() {
    String schemaName = createSchema("spark_empty_tbl");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);

    List<Row> rows = sql("SELECT id, score FROM %s.%s ORDER BY id", schemaName, tableName);
    Assertions.assertTrue(rows.isEmpty(), "Expected no rows for a newly created empty table");
  }

  @Test
  public void testSelectWithFilterViaSpark() {
    String schemaName = createSchema("spark_filter_tbl");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);
    sql(
        "INSERT INTO %s.%s VALUES "
            + "(1, CAST(1.1 AS FLOAT)), "
            + "(2, CAST(2.2 AS FLOAT)), "
            + "(3, CAST(3.3 AS FLOAT)), "
            + "(4, CAST(4.4 AS FLOAT))",
        schemaName, tableName);

    List<Row> rows =
        sql(
            "SELECT id, score FROM %s.%s "
                + "WHERE id >= 2 AND score < CAST(4.0 AS FLOAT) ORDER BY id",
            schemaName, tableName);
    Assertions.assertEquals(2, rows.size());
    assertRow(rows.get(0), 2, 2.2f);
    assertRow(rows.get(1), 3, 3.3f);
  }

  @Test
  public void testDropTableViaSpark() {
    String schemaName = createSchema("spark_drop_tbl");
    String tableName = newTableName("orders");
    NameIdentifier tableIdentifier = NameIdentifier.of(schemaName, tableName);

    createLanceTable(schemaName, tableName);
    Assertions.assertTrue(catalog.asTableCatalog().tableExists(tableIdentifier));

    sql("DROP TABLE %s.%s", schemaName, tableName);

    Assertions.assertFalse(catalog.asTableCatalog().tableExists(tableIdentifier));
    List<Row> tables = sql("SHOW TABLES IN %s", schemaName);
    Assertions.assertFalse(
        hasTable(tables, schemaName, tableName),
        String.format(
            "Expected table %s removed from schema %s, SHOW TABLES result: %s",
            tableName, schemaName, tables));
  }

  @Test
  public void testDropDatabaseViaSpark() {
    String schemaName = createSchema("spark_drop_ns");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

    sql("DROP DATABASE %s CASCADE", schemaName);
    createdSchemas.remove(schemaName);

    Assertions.assertFalse(catalog.asSchemas().schemaExists(schemaName));
    Assertions.assertFalse(listNamespaces().contains(schemaName));
  }

  @Test
  public void testCreateDuplicateTableFails() {
    String schemaName = createSchema("spark_duplicate_tbl");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);

    Throwable throwable =
        Assertions.assertThrows(Throwable.class, () -> createLanceTable(schemaName, tableName));
    assertSparkAnalysisFailure(throwable, "already exists", "table already exists");
  }

  @Test
  public void testCreateTableInNonExistentSchemaFails() {
    String nonExistentSchemaName = newSchemaName("spark_missing_ns");
    String tableName = newTableName("orders");

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> createLanceTable(nonExistentSchemaName, tableName));
    assertFailureContainsAll(
        exception, "NoSuchSchemaException", nonExistentSchemaName, "does not exist");
    Assertions.assertFalse(catalog.asSchemas().schemaExists(nonExistentSchemaName));
  }

  @Test
  public void testSelectFromNonExistentTableFails() {
    String schemaName = createSchema("spark_missing_tbl");
    String nonExistentTableName = newTableName("orders");

    Throwable throwable =
        Assertions.assertThrows(
            Throwable.class, () -> sql("SELECT * FROM %s.%s", schemaName, nonExistentTableName));
    assertFailureContainsAll(throwable, nonExistentTableName);
    assertFailureContainsAny(
        throwable,
        "Failed to describe table",
        "TABLE_OR_VIEW_NOT_FOUND",
        "not found",
        "does not exist",
        "cannot be found");
  }

  @Test
  public void testAlterTableRenameColumnUnsupportedViaSpark() {
    String schemaName = createSchema("spark_alter_column");
    String tableName = newTableName("orders");

    createLanceTable(schemaName, tableName);

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                sql("ALTER TABLE %s.%s RENAME COLUMN score TO final_score", schemaName, tableName));
    Assertions.assertTrue(
        exception.getMessage().toLowerCase(Locale.ROOT).contains("not supported"),
        "Expected unsupported-operation message, but got: " + exception.getMessage());

    sql("INSERT INTO %s.%s VALUES (1, CAST(9.9 AS FLOAT))", schemaName, tableName);
    List<Row> rows = sql("SELECT id, score FROM %s.%s ORDER BY id", schemaName, tableName);
    Assertions.assertEquals(1, rows.size());
    assertRow(rows.get(0), 1, 9.9f);

    Throwable renamedColumnThrowable =
        Assertions.assertThrows(
            Throwable.class, () -> sql("SELECT final_score FROM %s.%s", schemaName, tableName));
    assertFailureContainsAll(renamedColumnThrowable, "final_score");
    assertFailureContainsAny(
        renamedColumnThrowable,
        "cannot resolve",
        "cannot be resolved",
        "not found",
        "does not exist");
  }

  private GravitinoMetalake createMetalake(String metalakeName) {
    return client.createMetalake(metalakeName, "metalake for lance spark rest service tests", null);
  }

  private Catalog createCatalog(String catalogName) {
    return metalake.createCatalog(
        catalogName,
        Catalog.Type.RELATIONAL,
        "lakehouse-generic",
        "catalog for lance spark rest service tests",
        ImmutableMap.of(Catalog.PROPERTY_LOCATION, tempDir.toString()));
  }

  private SparkSession createSparkSession() {
    String bundleJarPath = System.getProperty(LANCE_SPARK_BUNDLE_JAR_PATH_PROPERTY);
    if (bundleJarPath == null || bundleJarPath.isEmpty()) {
      throw new IllegalStateException(
          "Missing Spark bundle jar path, expected system property "
              + LANCE_SPARK_BUNDLE_JAR_PATH_PROPERTY);
    }

    this.sparkClientClassLoader = createSparkClientClassLoader(bundleJarPath);
    assertSparkClientClassLoaderUsesBundle(bundleJarPath);

    SparkConf sparkConf =
        new SparkConf()
            .set("spark.jars", bundleJarPath)
            .set("spark.driver.extraClassPath", bundleJarPath)
            .set("spark.executor.extraClassPath", bundleJarPath)
            .set("spark.sql.catalog." + LANCE_SPARK_CATALOG, LANCE_SPARK_CATALOG_CLASS)
            .set("spark.sql.catalog." + LANCE_SPARK_CATALOG + ".impl", "rest")
            .set("spark.sql.catalog." + LANCE_SPARK_CATALOG + ".uri", getLanceRestServiceUrl())
            .set("spark.sql.catalog." + LANCE_SPARK_CATALOG + ".parent", CATALOG_NAME)
            .set("spark.sql.defaultCatalog", LANCE_SPARK_CATALOG)
            .set("spark.driver.extraJavaOptions", JAVA_MODULE_OPEN_OPTIONS)
            .set("spark.executor.extraJavaOptions", JAVA_MODULE_OPEN_OPTIONS)
            .set("spark.ui.enabled", "false");

    // Load the bundle the same way an external Spark client does instead of relying on test
    // classpath.
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(sparkClientClassLoader);
    try {
      return SparkSession.builder()
          .appName(getClass().getSimpleName())
          .master("local[1]")
          .config(sparkConf)
          .getOrCreate();
    } catch (RuntimeException | Error e) {
      try {
        stopSparkClientClassLoader();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      throw e;
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private void stopSparkSession() {
    if (sparkSession != null) {
      sparkSession.close();
      sparkSession = null;
    }
  }

  private void stopSparkClientClassLoader() throws IOException {
    if (sparkClientClassLoader != null) {
      sparkClientClassLoader.close();
      sparkClientClassLoader = null;
    }
  }

  private URLClassLoader createSparkClientClassLoader(String bundleJarPath) {
    try {
      URL bundleJarUrl = Path.of(bundleJarPath).toUri().toURL();
      return new LanceSparkBundleClassLoader(
          bundleJarUrl, Thread.currentThread().getContextClassLoader());
    } catch (MalformedURLException e) {
      throw new IllegalStateException("Failed to create Spark client classloader", e);
    }
  }

  private void assertSparkClientClassLoaderUsesBundle(String bundleJarPath) {
    try {
      Class<?> jniLoaderClass = sparkClientClassLoader.loadClass("org.lance.JniLoader");
      Assertions.assertSame(
          sparkClientClassLoader,
          jniLoaderClass.getClassLoader(),
          "Expected Lance JNI loader to come from the Spark bundle classloader");

      URL nativeResource = sparkClientClassLoader.getResource(LANCE_NATIVE_RESOURCE_PREFIX);
      Assertions.assertNotNull(
          nativeResource, "Expected Lance native resources in the Spark bundle");
      Assertions.assertTrue(
          nativeResource.toExternalForm().contains(Path.of(bundleJarPath).getFileName().toString()),
          "Expected Lance native resources to come from the Spark bundle, but got: "
              + nativeResource);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to load Lance JNI loader from Spark bundle", e);
    }
  }

  @FormatMethod
  private List<Row> sql(String sqlPattern, Object... args) {
    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(sparkClientClassLoader);
    try {
      return sparkSession.sql(String.format(Locale.ROOT, sqlPattern, args)).collectAsList();
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  private String createSchema(String schemaNamePrefix) {
    String schemaName = newSchemaName(schemaNamePrefix);
    sql("CREATE DATABASE %s", schemaName);
    createdSchemas.add(schemaName);
    return schemaName;
  }

  private void createLanceTable(String schemaName, String tableName) {
    sql(
        "CREATE TABLE %s.%s %s USING lance TBLPROPERTIES ('format'='lance')",
        schemaName, tableName, TABLE_COLUMNS);
  }

  private void dropSchema(String schemaName) {
    if (sparkSession != null) {
      try {
        sql("DROP DATABASE IF EXISTS %s CASCADE", schemaName);
        return;
      } catch (RuntimeException e) {
        if (catalog == null) {
          throw e;
        }
      }
    }

    if (catalog != null && catalog.asSchemas().schemaExists(schemaName)) {
      catalog.asSchemas().dropSchema(schemaName, true);
    }
  }

  private List<String> listNamespaces() {
    return sql("SHOW NAMESPACES IN %s", LANCE_SPARK_CATALOG).stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
  }

  private String newSchemaName(String prefix) {
    return GravitinoITUtils.genRandomName(prefix);
  }

  private String newTableName(String prefix) {
    return GravitinoITUtils.genRandomName(prefix);
  }

  private void assertTableLocationAndFormat(Table table, String schemaName, String tableName) {
    String persistedLocation = table.properties().get(Table.PROPERTY_LOCATION);

    Assertions.assertEquals(LanceConstants.LANCE_TABLE_FORMAT, table.properties().get("format"));
    Assertions.assertNotNull(persistedLocation);

    String normalizedPersistedLocation = normalizeLocation(persistedLocation);
    String normalizedSchemaPath = normalizeLocation(tempDir.resolve(schemaName).toString());
    Assertions.assertTrue(normalizedPersistedLocation.startsWith(normalizedSchemaPath));
    Assertions.assertTrue(normalizedPersistedLocation.contains(tableName));
    Assertions.assertTrue(Files.exists(Path.of(persistedLocation)));
  }

  private void assertRow(Row row, int expectedId, float expectedScore) {
    Assertions.assertEquals(expectedId, row.<Integer>getAs("id"));
    // Spark may materialize FLOAT columns through different numeric wrappers across execution
    // paths.
    Assertions.assertEquals(expectedScore, ((Number) row.getAs("score")).floatValue(), 0.001f);
  }

  private boolean hasTable(List<Row> tables, String schemaName, String tableName) {
    String expectedSchema = schemaName.toLowerCase(Locale.ROOT);
    String expectedTable = tableName.toLowerCase(Locale.ROOT);

    for (Row row : tables) {
      Map<String, String> rowValues = extractShowTableRowValues(row);
      String actualSchema = rowValues.get("namespace");
      String rawTableName = rowValues.get("tableName");
      if (actualSchema == null || rawTableName == null) {
        continue;
      }

      if (expectedSchema.equals(actualSchema.toLowerCase(Locale.ROOT))
          && expectedTable.equals(extractLeafTableName(rawTableName).toLowerCase(Locale.ROOT))) {
        return true;
      }
    }

    return false;
  }

  private Map<String, String> extractShowTableRowValues(Row row) {
    // SHOW TABLES rows can be exposed either as named columns or positional values.
    Map<String, String> values = new LinkedHashMap<>();
    values.put("namespace", getStringColumn(row, "namespace", 0));
    values.put("tableName", getStringColumn(row, "tableName", 1));
    return values;
  }

  private String getStringColumn(Row row, String columnName, int fallbackIndex) {
    try {
      Object value = row.getAs(columnName);
      if (value != null) {
        return value.toString();
      }
    } catch (IllegalArgumentException ignored) {
      // Fall back to index access if column name is not present.
    }

    if (fallbackIndex >= 0 && fallbackIndex < row.size()) {
      Object value = row.get(fallbackIndex);
      return value == null ? null : value.toString();
    }

    return null;
  }

  private String extractLeafTableName(String rawTableName) {
    int delimiterIndex = Math.max(rawTableName.lastIndexOf('$'), rawTableName.lastIndexOf('.'));
    if (delimiterIndex < 0) {
      return rawTableName;
    }

    return rawTableName.substring(delimiterIndex + 1);
  }

  private void assertSparkAnalysisFailure(Throwable throwable, String... expectedMessageParts) {
    Assertions.assertTrue(
        hasCause(throwable, AnalysisException.class),
        String.format(
            "Expected AnalysisException in cause chain, but got %s with message: %s",
            throwable.getClass().getName(), throwable.getMessage()));
    Assertions.assertTrue(
        containsAnyMessagePart(throwable, expectedMessageParts),
        String.format(
            "Expected error message to contain one of %s, but message was: %s",
            Arrays.toString(expectedMessageParts), throwable.getMessage()));
  }

  private void assertFailureContainsAll(Throwable throwable, String... expectedMessageParts) {
    for (String expectedMessagePart : expectedMessageParts) {
      Assertions.assertTrue(
          containsMessagePart(throwable, expectedMessagePart),
          String.format(
              "Expected error message to contain \"%s\", but message was: %s",
              expectedMessagePart, throwable.getMessage()));
    }
  }

  private void assertFailureContainsAny(Throwable throwable, String... expectedMessageParts) {
    Assertions.assertTrue(
        containsAnyMessagePart(throwable, expectedMessageParts),
        String.format(
            "Expected error message to contain one of %s, but message was: %s",
            Arrays.toString(expectedMessageParts), throwable.getMessage()));
  }

  private boolean hasCause(Throwable throwable, Class<? extends Throwable> expectedType) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (expectedType.isInstance(current)) {
        return true;
      }
    }

    return false;
  }

  private boolean containsAnyMessagePart(Throwable throwable, String... expectedMessageParts) {
    for (String expectedMessagePart : expectedMessageParts) {
      if (containsMessagePart(throwable, expectedMessagePart)) {
        return true;
      }
    }

    return false;
  }

  private boolean containsMessagePart(Throwable throwable, String expectedMessagePart) {
    String expected = expectedMessagePart.toLowerCase(Locale.ROOT);
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      String message = current.getMessage();
      if (message == null) {
        continue;
      }

      String normalizedMessage = message.toLowerCase(Locale.ROOT);
      if (normalizedMessage.contains(expected)) {
        return true;
      }
    }

    return false;
  }

  private Exception appendFailure(Exception failure, Exception candidate) {
    if (failure == null) {
      return candidate;
    }

    failure.addSuppressed(candidate);
    return failure;
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://localhost:%d/lance", getLanceRESTServerPort());
  }

  private static String normalizeLocation(String location) {
    if (location.endsWith("/")) {
      return location.substring(0, location.length() - 1);
    }

    return location;
  }

  private static class LanceSparkBundleClassLoader extends URLClassLoader {

    private LanceSparkBundleClassLoader(URL bundleJarUrl, ClassLoader parent) {
      super(new URL[] {bundleJarUrl}, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
          loadedClass = loadUnresolvedClass(name);
        }

        if (resolve) {
          resolveClass(loadedClass);
        }

        return loadedClass;
      }
    }

    @Override
    public URL getResource(String name) {
      if (isLanceNativeResource(name)) {
        URL resource = findResource(name);
        if (resource != null) {
          return resource;
        }
      }

      return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
      if (!isLanceNativeResource(name)) {
        return super.getResources(name);
      }

      List<URL> resources = new ArrayList<>();
      resources.addAll(Collections.list(findResources(name)));

      ClassLoader parent = getParent();
      Enumeration<URL> parentResources =
          parent == null ? ClassLoader.getSystemResources(name) : parent.getResources(name);
      resources.addAll(Collections.list(parentResources));

      return Collections.enumeration(resources);
    }

    private Class<?> loadUnresolvedClass(String name) throws ClassNotFoundException {
      if (isLanceBundleClass(name)) {
        try {
          return findClass(name);
        } catch (ClassNotFoundException e) {
          return super.loadClass(name, false);
        }
      }

      return super.loadClass(name, false);
    }

    private boolean isLanceBundleClass(String name) {
      return name.startsWith(LANCE_CLASS_PREFIX)
          || name.startsWith(LANCE_NATIVE_LOADER_CLASS_PREFIX);
    }

    private boolean isLanceNativeResource(String name) {
      return name.startsWith(LANCE_NATIVE_RESOURCE_PREFIX);
    }
  }
}
