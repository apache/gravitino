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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
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
import org.apache.gravitino.spark.connector.utils.LocalSparkSessions;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoGlueCatalog {

  private GravitinoGlueCatalog gravitinoGlueCatalog;

  @BeforeAll
  void initCatalogManager() {
    // GravitinoGlueCatalog extends BaseCatalog which calls GravitinoCatalogManager.get()
    // in its constructor, so we must initialize the manager first.
    GravitinoClient mockClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> mockClient);
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
        };

    org.apache.spark.sql.connector.catalog.Table result =
        catalog.loadSparkTable(ident, mockGravitinoTable);
    Assertions.assertSame(mockSparkTable, result);
  }

  // -------------------------------------------------------------------------
  // Test applyS3Credential (credential vending)
  // -------------------------------------------------------------------------

  @Test
  void testApplyS3CredentialInjectsFilesystemKeysAndGlueApiKeys() {
    Catalog mockCatalog = mock(Catalog.class);
    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("test-access-key", "test-secret-key");
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {s3Cred});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props, null);

    Assertions.assertEquals("test-access-key", props.get("fs.s3a.access.key"));
    Assertions.assertEquals("test-secret-key", props.get("fs.s3a.secret.key"));
    Assertions.assertEquals(
        "test-access-key", vended.get(GluePropertiesConverter.AWS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "test-secret-key", vended.get(GluePropertiesConverter.AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testApplyS3CredentialUsesAwsKeysForGlueAndS3KeysForStorage() {
    Catalog mockCatalog = mock(Catalog.class);
    AwsSecretKeyCredential awsCred = new AwsSecretKeyCredential("glue-ak", "glue-sk");
    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("s3-ak", "s3-sk");
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {awsCred, s3Cred});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props, null);

    Assertions.assertEquals("s3-ak", props.get("fs.s3a.access.key"));
    Assertions.assertEquals("s3-sk", props.get("fs.s3a.secret.key"));
    Assertions.assertEquals("glue-ak", vended.get(GluePropertiesConverter.AWS_ACCESS_KEY_ID));
    Assertions.assertEquals("glue-sk", vended.get(GluePropertiesConverter.AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testApplyS3CredentialReturnsEmptyWhenNoCredentials() {
    Catalog mockCatalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props, null);

    Assertions.assertTrue(props.isEmpty());
    Assertions.assertTrue(vended.isEmpty());
  }

  @Test
  void testApplyS3CredentialKeepsConfiguredS3Keys() {
    Catalog mockCatalog = mock(Catalog.class);
    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("vended-ak", "vended-sk");
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {s3Cred});
    Map<String, String> configured =
        ImmutableMap.of("fs.s3a.access.key", "user-ak", "fs.s3a.secret.key", "user-sk");
    Map<String, String> props = new HashMap<>(configured);

    GravitinoGlueCatalog.applyS3Credential(mockCatalog, props, null);

    Assertions.assertEquals(configured, props, "no vended key is written, under any name");
  }

  /**
   * The options handed to the kyuubi catalog carry the vended keys only where the user configured
   * none. Keys arriving as {@code spark.bypass.*} catalog properties count as configured: the Glue
   * documentation offers them as an alternative for S3 access.
   */
  @Test
  void testConfiguredBypassKeysKeepTheVendedPairOutOfTheKyuubiCatalogOptions() {
    HiveTableCatalog kyuubi = mock(HiveTableCatalog.class);
    TestableGlueCatalog catalog = newGlueCatalogVending(kyuubi, "vended-ak", "vended-sk");

    catalog.initializeKyuubiCatalog(ImmutableMap.of(), ImmutableMap.of());
    CaseInsensitiveStringMap vended = capturedOptions(kyuubi);
    Assertions.assertEquals("vended-ak", vended.get("fs.s3a.access.key"));
    Assertions.assertEquals("vended-sk", vended.get("fs.s3a.secret.key"));

    HiveTableCatalog configuredKyuubi = mock(HiveTableCatalog.class);
    newGlueCatalogVending(configuredKyuubi, "vended-ak", "vended-sk")
        .initializeKyuubiCatalog(
            ImmutableMap.of(),
            ImmutableMap.of(
                "spark.bypass.fs.s3a.access.key", "bypass-ak",
                "spark.bypass.fs.s3a.secret.key", "bypass-sk"));
    CaseInsensitiveStringMap kept = capturedOptions(configuredKyuubi);
    Assertions.assertEquals("bypass-ak", kept.get("fs.s3a.access.key"));
    Assertions.assertEquals("bypass-sk", kept.get("fs.s3a.secret.key"));
  }

  /**
   * The {@code fs.s3a.*} pair is treated as a unit, blank values do not count, and a differently
   * cased entry is still one the user made, as on the Hive catalog.
   */
  @Test
  void testApplyS3CredentialWritesThePairOnlyAsAWholeAndOnlyWhereUnconfigured() {
    Catalog client = catalogVending("vended-ak", "vended-sk");

    Map<String, String> withAccessKeyOnly =
        new HashMap<>(ImmutableMap.of("fs.s3a.access.key", "user-ak"));
    GravitinoGlueCatalog.applyS3Credential(client, withAccessKeyOnly, null);
    Assertions.assertEquals(
        ImmutableMap.of("fs.s3a.access.key", "user-ak"),
        withAccessKeyOnly,
        "a vended secret next to a configured access key would not authenticate");

    Map<String, String> withSecretKeyOnly =
        new HashMap<>(ImmutableMap.of("fs.s3a.secret.key", "user-sk"));
    GravitinoGlueCatalog.applyS3Credential(client, withSecretKeyOnly, null);
    Assertions.assertEquals(
        ImmutableMap.of("fs.s3a.secret.key", "user-sk"),
        withSecretKeyOnly,
        "a vended access key next to a configured secret would not authenticate");

    Map<String, String> withBlanks = new HashMap<>(ImmutableMap.of("FS.S3A.SECRET.KEY", " "));
    GravitinoGlueCatalog.applyS3Credential(client, withBlanks, null);
    Assertions.assertEquals(
        ImmutableMap.of("fs.s3a.access.key", "vended-ak", "fs.s3a.secret.key", "vended-sk"),
        withBlanks,
        "a blank value is replaced");

    Map<String, String> differentlyCased =
        new HashMap<>(ImmutableMap.of("FS.S3A.ACCESS.KEY", "user-ak"));
    GravitinoGlueCatalog.applyS3Credential(client, differentlyCased, null);
    Assertions.assertEquals(
        ImmutableMap.of("FS.S3A.ACCESS.KEY", "user-ak"),
        differentlyCased,
        "no vended key is written beside an entry the user made, whatever its case");
  }

  /**
   * A key set on the session counts through the real catalog path too: were the catalog to skip the
   * session, the vended pair would land in the options and kyuubi would apply it over the session's
   * keys.
   */
  @Test
  @Timeout(60)
  void testSessionKeysWinThroughTheRealCatalogPath() {
    try (LocalSparkSessions.Session local =
        LocalSparkSessions.create("test-glue-catalog-credentials", ImmutableMap.of())) {
      SparkSession spark = local.spark();
      spark.conf().set("fs.s3a.access.key", "runtime-ak");
      spark.conf().set("fs.s3a.secret.key", "runtime-sk");

      HiveTableCatalog kyuubi = mock(HiveTableCatalog.class);
      newGlueCatalogVending(kyuubi, "vended-ak", "vended-sk")
          .initializeKyuubiCatalog(ImmutableMap.of(), ImmutableMap.of());
      CaseInsensitiveStringMap options = capturedOptions(kyuubi);

      Assertions.assertNull(options.get("fs.s3a.access.key"), "the session's key must win");
      Assertions.assertNull(options.get("fs.s3a.secret.key"), "the session's key must win");
    }
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
  void testIcebergTablesUseIcebergTimestampTypes() {
    SparkTypeConverter converter =
        gravitinoGlueCatalog.getSparkTypeConverter(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));

    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), converter.toGravitinoType(DataTypes.TimestampType));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(),
        converter.toGravitinoType(DataTypes.TimestampNTZType));
    Assertions.assertEquals(
        DataTypes.TimestampNTZType, converter.toSparkType(Types.TimestampType.withoutTimeZone(6)));
  }

  @Test
  void testTablesWithoutFormatFollowCatalogDefaultTableFormat() throws Exception {
    Field catalogProperties = GravitinoGlueCatalog.class.getDeclaredField("catalogProperties");
    catalogProperties.setAccessible(true);
    catalogProperties.set(
        gravitinoGlueCatalog, ImmutableMap.of(GlueConstants.DEFAULT_TABLE_FORMAT, "iceberg"));

    SparkTypeConverter converter = gravitinoGlueCatalog.getSparkTypeConverter(ImmutableMap.of());

    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(), converter.toGravitinoType(DataTypes.TimestampType));
  }

  @Test
  void testIcebergTableChangesUseIcebergTimestampTypes() {
    Table icebergTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          @Override
          protected Table loadGravitinoTable(Identifier ident) {
            return icebergTable;
          }
        };

    TableChange addColumn = TableChange.addColumn(new String[] {"ts"}, DataTypes.TimestampType);

    org.apache.gravitino.rel.TableChange change =
        catalog
            .getSparkTableChangeConverter(
                Identifier.of(new String[] {"db"}, "tbl"), new TableChange[] {addColumn})
            .toGravitinoTableChange(addColumn);

    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(),
        ((org.apache.gravitino.rel.TableChange.AddColumn) change).getDataType());
  }

  @Test
  void testAlterIcebergTableAddsColumnWithIcebergTimestampType() throws Exception {
    Table icebergTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));
    org.apache.gravitino.rel.TableCatalog tableCatalog =
        mock(org.apache.gravitino.rel.TableCatalog.class);
    Catalog client = mock(Catalog.class);
    when(client.asTableCatalog()).thenReturn(tableCatalog);
    TableCatalog mockSparkCatalog = mock(TableCatalog.class);
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockSparkCatalog;
          }

          @Override
          protected Catalog gravitinoCatalog() {
            return client;
          }

          @Override
          protected Table loadGravitinoTable(Identifier ident) {
            return icebergTable;
          }

          @Override
          protected org.apache.spark.sql.connector.catalog.Table loadSparkTable(
              Identifier ident, Table gravitinoTable) {
            return null;
          }

          @Override
          protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
              Identifier identifier,
              Table gravitinoTable,
              org.apache.spark.sql.connector.catalog.Table sparkTable,
              TableCatalog sparkHiveCatalog,
              PropertiesConverter propertiesConverter,
              SparkTransformConverter sparkTransformConverter,
              SparkTypeConverter sparkTypeConverter) {
            return null;
          }
        };

    catalog.alterTable(
        Identifier.of(new String[] {"db"}, "tbl"),
        TableChange.addColumn(new String[] {"ts"}, DataTypes.TimestampType));

    ArgumentCaptor<org.apache.gravitino.rel.TableChange> change =
        ArgumentCaptor.forClass(org.apache.gravitino.rel.TableChange.class);
    verify(tableCatalog).alterTable(any(), change.capture());
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(),
        ((org.apache.gravitino.rel.TableChange.AddColumn) change.getValue()).getDataType());
  }

  @Test
  void testTableIsLoadedOnlyForChangesThatSetTimestampTypes() {
    Table icebergTable =
        createMockGravitinoTable(
            ImmutableMap.of(GlueConstants.TABLE_FORMAT, GlueConstants.TABLE_FORMAT_ICEBERG));
    AtomicInteger loads = new AtomicInteger();
    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          @Override
          protected Table loadGravitinoTable(Identifier ident) {
            loads.incrementAndGet();
            return icebergTable;
          }
        };
    Identifier ident = Identifier.of(new String[] {"db"}, "tbl");
    String[] column = {"c"};

    catalog.getSparkTableChangeConverter(
        ident,
        new TableChange[] {
          TableChange.setProperty("comment", "c"),
          TableChange.addColumn(column, DataTypes.LongType),
          TableChange.updateColumnType(column, DataTypes.StringType)
        });
    Assertions.assertEquals(0, loads.get());

    catalog.getSparkTableChangeConverter(
        ident,
        new TableChange[] {
          TableChange.addColumn(column, DataTypes.createArrayType(DataTypes.TimestampType))
        });
    Assertions.assertEquals(1, loads.get());

    catalog.getSparkTableChangeConverter(
        ident,
        new TableChange[] {TableChange.updateColumnType(column, DataTypes.TimestampNTZType)});
    Assertions.assertEquals(2, loads.get());
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
  // One loadTable must cost one Gravitino round trip: the loadSparkTable hook receives
  // the already-fetched table instead of re-fetching it for Iceberg/Hive routing.
  // -------------------------------------------------------------------------

  @Test
  void testLoadTableFetchesGravitinoTableOnce() throws Exception {
    Catalog client = mock(Catalog.class);
    org.apache.gravitino.rel.TableCatalog gravitinoTableCatalog =
        mock(org.apache.gravitino.rel.TableCatalog.class);
    when(client.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    Table gravitinoTable = createMockGravitinoTable(ImmutableMap.of());
    when(gravitinoTableCatalog.loadTable(any(), any())).thenReturn(gravitinoTable);
    TableCatalog mockSparkCatalog = mock(TableCatalog.class);
    when(mockSparkCatalog.loadTable(any()))
        .thenReturn(mock(org.apache.spark.sql.connector.catalog.Table.class));

    GravitinoGlueCatalog catalog =
        new GravitinoGlueCatalog() {
          {
            sparkCatalog = mockSparkCatalog;
          }

          @Override
          protected Catalog gravitinoCatalog() {
            return client;
          }

          @Override
          protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
              Identifier identifier,
              Table gravitinoTable,
              org.apache.spark.sql.connector.catalog.Table sparkTable,
              TableCatalog sparkHiveCatalog,
              PropertiesConverter propertiesConverter,
              SparkTransformConverter sparkTransformConverter,
              SparkTypeConverter sparkTypeConverter) {
            return sparkTable;
          }
        };

    catalog.loadTable(Identifier.of(new String[] {"db"}, "tbl"));

    verify(gravitinoTableCatalog, times(1)).loadTable(any(), any());
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

  private static Catalog catalogVending(String accessKey, String secretKey) {
    Catalog client = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(client.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials())
        .thenReturn(new Credential[] {new S3SecretKeyCredential(accessKey, secretKey)});
    return client;
  }

  private static TestableGlueCatalog newGlueCatalogVending(
      HiveTableCatalog kyuubi, String accessKey, String secretKey) {
    return new TestableGlueCatalog(catalogVending(accessKey, secretKey), kyuubi);
  }

  private static CaseInsensitiveStringMap capturedOptions(HiveTableCatalog kyuubi) {
    ArgumentCaptor<CaseInsensitiveStringMap> options =
        ArgumentCaptor.forClass(CaseInsensitiveStringMap.class);
    verify(kyuubi).initialize(any(), options.capture());
    return options.getValue();
  }

  /** A Glue catalog over a mocked Gravitino client and kyuubi catalog, exposing the init path. */
  private static final class TestableGlueCatalog extends GravitinoGlueCatalog {

    private final Catalog client;
    private final HiveTableCatalog kyuubi;

    private TestableGlueCatalog(Catalog client, HiveTableCatalog kyuubi) {
      this.client = client;
      this.kyuubi = kyuubi;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return client;
    }

    @Override
    protected HiveTableCatalog createHiveTableCatalog() {
      return kyuubi;
    }

    private void initializeKyuubiCatalog(
        Map<String, String> options, Map<String, String> properties) {
      createAndInitSparkCatalog("glue", new CaseInsensitiveStringMap(options), properties);
    }
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
