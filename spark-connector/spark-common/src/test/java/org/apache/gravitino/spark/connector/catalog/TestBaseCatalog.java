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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBaseCatalog {

  private static final String FORMAT_NAMESPACE = "parquet";
  private static final String TEST_CATALOG = "datasource_test";

  @TempDir private static File tempDir;

  private static SparkSession spark;
  private static FileSystem localFileSystem;
  private static Path originalWorkingDirectory;

  private SupportsSchemas schemas;
  private org.apache.gravitino.rel.TableCatalog gravitinoTableCatalog;
  private BaseCatalog catalog;

  @BeforeAll
  static void initCatalogManagerAndSpark() throws IOException {
    GravitinoClient gravitinoClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);

    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-datasource-resolution")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog." + TEST_CATALOG, AnalyzerCatalog.class.getName())
            .getOrCreate();
    spark.catalog().setCurrentCatalog(TEST_CATALOG);
    localFileSystem = FileSystem.get(spark.sparkContext().hadoopConfiguration());
    originalWorkingDirectory = localFileSystem.getWorkingDirectory();
    localFileSystem.setWorkingDirectory(new Path(tempDir.toURI()));
    spark.range(1).write().parquet("data.parquet");
  }

  @AfterAll
  static void cleanupCatalogManagerAndSpark() {
    if (localFileSystem != null && originalWorkingDirectory != null) {
      localFileSystem.setWorkingDirectory(originalWorkingDirectory);
    }
    if (spark != null) {
      spark.stop();
    }
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    schemas = mock(SupportsSchemas.class);
    gravitinoTableCatalog = mock(org.apache.gravitino.rel.TableCatalog.class);
    when(gravitinoCatalog.asSchemas()).thenReturn(schemas);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    when(schemas.loadSchema(FORMAT_NAMESPACE))
        .thenThrow(new NoSuchSchemaException("Schema parquet does not exist"));
    when(gravitinoTableCatalog.loadTable(any(NameIdentifier.class), anySet()))
        .thenThrow(new IllegalArgumentException("Invalid table name"));
    catalog = new DataSourceTestCatalog(gravitinoCatalog);
    AnalyzerCatalog.gravitinoCatalog = gravitinoCatalog;
    localFileSystem.setWorkingDirectory(new Path(tempDir.toURI()));
  }

  @Test
  void pathLikeNamesMatchThePrefilter() {
    Identifier[] pathIdents = {
      Identifier.of(new String[] {"parquet"}, "/tmp/data"),
      Identifier.of(new String[] {"parquet"}, "data.parquet"),
      Identifier.of(new String[] {"csv"}, "s3a://bucket/file.csv"),
      Identifier.of(new String[] {"json"}, "relative/path/to/file"),
      Identifier.of(new String[] {"PARQUET"}, "hdfs://namenode/path"),
      Identifier.of(new String[] {"binaryFile"}, "C:\\Users\\data\\file"),
    };
    for (Identifier ident : pathIdents) {
      assertTrue(
          BaseCatalog.looksLikeDataSourceReference(ident),
          "Expected " + ident + " to match the DataSource prefilter");
    }
  }

  @Test
  void regularTableNamesDoNotMatchThePrefilter() {
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(Identifier.of(new String[] {"parquet"}, "users")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(
            Identifier.of(new String[] {"my_schema"}, "my_table")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(
            Identifier.of(new String[] {"parquet", "extra"}, "table")));
    assertFalse(
        BaseCatalog.looksLikeDataSourceReference(Identifier.of(new String[] {}, "parquet")));
  }

  @Test
  void existingTableWithDataSourceSyntaxTakesPrecedence() {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "table/with/slash");
    when(gravitinoTableCatalog.loadTable(any(NameIdentifier.class), anySet()))
        .thenReturn(mock(org.apache.gravitino.rel.Table.class));

    assertTrue(catalog.tableExists(ident));
  }

  @Test
  void loadTableYieldsForInvalidDataSourceName() {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident));
  }

  @Test
  void loadTableForWritingYieldsForInvalidDataSourceName() {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertThrows(NoSuchTableException.class, () -> catalog.loadTable(ident, java.util.Set.of()));
  }

  @Test
  void tableExistsReturnsFalseForInvalidDataSourceName() {
    Identifier ident = Identifier.of(new String[] {FORMAT_NAMESPACE}, "s3a://bucket/file.parquet");
    assertFalse(catalog.tableExists(ident));
  }

  @Test
  void sparkAnalyzerResolvesRelativeFilename() {
    assertEquals(1L, spark.sql("SELECT * FROM parquet.`data.parquet`").count());
  }

  @Test
  void sparkAnalyzerResolvesPathWhenSchemaExists() {
    doReturn(mock(Schema.class)).when(schemas).loadSchema(FORMAT_NAMESPACE);

    String path = tempDir.toPath().resolve("data.parquet").toString();
    assertEquals(1L, spark.sql("SELECT * FROM parquet.`" + path + "`").count());
  }

  private static class DataSourceTestCatalog extends BaseCatalog {
    private final Catalog catalog;

    private DataSourceTestCatalog(Catalog catalog) {
      this.catalog = catalog;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return catalog;
    }

    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return null;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return null;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return null;
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return null;
    }
  }

  /** Catalog instantiated by Spark to exercise DataSource fallback through the real analyzer. */
  public static class AnalyzerCatalog extends DataSourceTestCatalog {
    private static Catalog gravitinoCatalog;

    public AnalyzerCatalog() {
      super(null);
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {}

    @Override
    public String name() {
      return TEST_CATALOG;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return gravitinoCatalog;
    }
  }
}
