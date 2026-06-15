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

package org.apache.gravitino.spark.connector.glue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoGlueCatalog {

  private GravitinoGlueCatalog gravitinoGlueCatalog;

  @BeforeAll
  void initCatalogManager() {
    // GravitinoGlueCatalog extends BaseCatalog which calls GravitinoCatalogManager.get()
    // in its constructor, so we must initialize the manager first.
    GravitinoClient mockClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(() -> mockClient);
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    gravitinoGlueCatalog = new GravitinoGlueCatalog();
  }

  // -------------------------------------------------------------------------
  // Test isIcebergTable (static package-private method)
  // -------------------------------------------------------------------------

  @Test
  void testIsIcebergTableWithIcebergFormat() {
    Table mockTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithIcebergFormatLowercase() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "iceberg"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithIcebergFormatMixedCase() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "IcEbErG"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithHiveFormat() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "HIVE"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithDeltaFormat() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, "DELTA"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithEmptyProperties() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of());
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNullProperties() {
    Table mockTable = mock(Table.class);
    when(mockTable.properties()).thenReturn(null);
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNoTableFormatProperty() {
    Table mockTable =
        createMockGravitinoTable(ImmutableMap.of("aws-location", "s3://bucket/table"));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithEmptyTableFormat() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of(GlueConstants.TABLE_FORMAT, ""));
    Assertions.assertFalse(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNativeIcebergTableType() {
    // Tables created directly by Iceberg GlueCatalog use table_type=ICEBERG (no table-format key).
    Table mockTable = createMockGravitinoTable(ImmutableMap.of("table_type", "ICEBERG"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  @Test
  void testIsIcebergTableWithNativeIcebergTableTypeLowercase() {
    Table mockTable = createMockGravitinoTable(ImmutableMap.of("table_type", "iceberg"));
    Assertions.assertTrue(GravitinoGlueCatalog.isIcebergTable(mockTable));
  }

  // -------------------------------------------------------------------------
  // Test loadSparkTable routes non-Iceberg tables to sparkCatalog directly
  // -------------------------------------------------------------------------

  @Test
  void testLoadSparkTableRoutesNonIcebergToSparkCatalog() throws Exception {
    org.apache.spark.sql.connector.catalog.TableCatalog mockCatalog =
        mock(org.apache.spark.sql.connector.catalog.TableCatalog.class);
    org.apache.spark.sql.connector.catalog.Table mockSparkTable =
        mock(org.apache.spark.sql.connector.catalog.Table.class);
    Table mockGravitinoTable = createMockGravitinoTable(ImmutableMap.of());

    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    when(mockCatalog.loadTable(any())).thenReturn(mockSparkTable);

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockCatalog;
          }

          @Override
          protected Table loadGravitinoTable(Identifier ident) throws NoSuchTableException {
            return mockGravitinoTable;
          }
        };

    org.apache.spark.sql.connector.catalog.Table result = catalog.loadSparkTable(ident);
    Assertions.assertSame(mockSparkTable, result);
  }

  // -------------------------------------------------------------------------
  // Test applyS3Credential (credential vending)
  // -------------------------------------------------------------------------

  @Test
  void testApplyS3CredentialInjectsHadoopProperties() {
    Catalog mockCatalog = mock(Catalog.class);
    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("test-access-key", "test-secret-key");
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {s3Cred});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props);

    Assertions.assertEquals("test-access-key", props.get("hadoop.fs.s3a.access.key"));
    Assertions.assertEquals("test-secret-key", props.get("hadoop.fs.s3a.secret.key"));
    Assertions.assertEquals(
        "test-access-key", vended.get(GluePropertiesConverter.AWS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "test-secret-key", vended.get(GluePropertiesConverter.AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testApplyS3CredentialReturnsEmptyWhenNoCredentials() {
    Catalog mockCatalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props);

    Assertions.assertTrue(props.isEmpty());
    Assertions.assertTrue(vended.isEmpty());
  }

  // -------------------------------------------------------------------------
  // Test converter methods
  // -------------------------------------------------------------------------

  @Test
  void testGetSparkTypeConverter() {
    SparkTypeConverter typeConverter = gravitinoGlueCatalog.getSparkTypeConverter();
    Assertions.assertNotNull(typeConverter);
    Assertions.assertInstanceOf(
        org.apache.gravitino.spark.connector.hive.SparkHiveTypeConverter.class, typeConverter);
  }

  @Test
  void testGetPropertiesConverter() {
    PropertiesConverter converter = gravitinoGlueCatalog.getPropertiesConverter();
    Assertions.assertNotNull(converter);
    Assertions.assertInstanceOf(GluePropertiesConverter.class, converter);
  }

  @Test
  void testGetSparkTransformConverter() {
    SparkTransformConverter transformer = gravitinoGlueCatalog.getSparkTransformConverter();
    Assertions.assertNotNull(transformer);
  }

  // -------------------------------------------------------------------------
  // Test invalidateTable propagates to icebergGlueCatalog
  // -------------------------------------------------------------------------

  @Test
  void testInvalidateTableCallsBothCatalogsWhenIcebergInitialized() throws Exception {
    TableCatalog mockSparkCatalog = mock(TableCatalog.class);
    SparkCatalog mockIcebergCatalog = mock(SparkCatalog.class);
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockSparkCatalog;
            icebergGlueCatalog = mockIcebergCatalog;
          }
        };

    catalog.invalidateTable(ident);

    verify(mockSparkCatalog).invalidateTable(ident);
    verify(mockIcebergCatalog).invalidateTable(ident);
  }

  @Test
  void testInvalidateTableCallsOnlySparkCatalogWhenIcebergNotInitialized() throws Exception {
    TableCatalog mockSparkCatalog = mock(TableCatalog.class);
    SparkCatalog mockIcebergCatalog = mock(SparkCatalog.class);
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockSparkCatalog;
            // icebergGlueCatalog left null (not initialized)
          }
        };

    catalog.invalidateTable(ident);

    verify(mockSparkCatalog).invalidateTable(ident);
    verify(mockIcebergCatalog, never()).invalidateTable(any());
  }

  // -------------------------------------------------------------------------
  // Test loadFunction delegation to Iceberg catalog
  // -------------------------------------------------------------------------

  @Test
  void testLoadFunctionDelegatesToIcebergCatalog() throws NoSuchFunctionException {
    SparkCatalog mockIcebergCatalog = mock(SparkCatalog.class);
    UnboundFunction mockFunction = mock(UnboundFunction.class);
    Identifier ident = Identifier.of(new String[] {}, "years");
    when(mockIcebergCatalog.loadFunction(ident)).thenReturn(mockFunction);

    Assertions.assertSame(
        mockFunction, makeGlueCatalogWithIceberg(mockIcebergCatalog).loadFunction(ident));
  }

  @Test
  void testLoadFunctionFallsBackToSuperWhenIcebergThrows() throws NoSuchFunctionException {
    SparkCatalog mockIcebergCatalog = mock(SparkCatalog.class);
    Identifier ident = Identifier.of(new String[] {}, "custom_func");
    doThrow(mock(NoSuchFunctionException.class)).when(mockIcebergCatalog).loadFunction(any());

    GravitinoGlueCatalog catalog = makeGlueCatalogWithIceberg(mockIcebergCatalog);
    // When Iceberg throws NoSuchFunctionException, the catch block calls super.loadFunction.
    // In the test environment, super also throws; verify Iceberg was consulted first.
    Assertions.assertThrows(Exception.class, () -> catalog.loadFunction(ident));
    verify(mockIcebergCatalog).loadFunction(ident);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  /** Creates a GravitinoGlueCatalog with the given Iceberg catalog pre-injected. */
  private GravitinoGlueCatalog makeGlueCatalogWithIceberg(SparkCatalog icebergCatalog) {
    return new GravitinoGlueCatalog() {
      {
        icebergGlueCatalog = icebergCatalog;
      }
    };
  }

  /** Creates a mock Gravitino Table with the given properties. */
  private Table createMockGravitinoTable(java.util.Map<String, String> properties) {
    Table mockTable = mock(Table.class);
    when(mockTable.properties()).thenReturn(new HashMap<>(properties));
    when(mockTable.name()).thenReturn("test_db.test_table");
    when(mockTable.columns())
        .thenReturn(new Column[] {Column.of("id", Types.IntegerType.get(), "id column")});
    return mockTable;
  }
}
