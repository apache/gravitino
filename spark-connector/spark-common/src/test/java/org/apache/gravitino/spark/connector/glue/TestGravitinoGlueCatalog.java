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
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
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
  void testApplyS3CredentialUsesAwsForGlueAndS3ForStorage() {
    Catalog mockCatalog = mock(Catalog.class);
    AwsSecretKeyCredential awsCred = new AwsSecretKeyCredential("glue-ak", "glue-sk");
    S3SecretKeyCredential s3Cred = new S3SecretKeyCredential("s3-ak", "s3-sk");
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(mockCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[] {awsCred, s3Cred});

    Map<String, String> props = new HashMap<>();
    Map<String, String> vended = GravitinoGlueCatalog.applyS3Credential(mockCatalog, props);

    Assertions.assertEquals("s3-ak", props.get("hadoop.fs.s3a.access.key"));
    Assertions.assertEquals("s3-sk", props.get("hadoop.fs.s3a.secret.key"));
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
  // A bucket transform on a Glue table is format-dependent: an Iceberg-format table gets an
  // Iceberg bucket partition (the server drops distributions, so leaving it a distribution would
  // silently create an unpartitioned table), a Hive-format table keeps it as bucketing.
  // -------------------------------------------------------------------------

  @Test
  void testBucketIsPartitioningWhenTheTableFormatIsIceberg() throws Exception {
    CreatedTable created =
        createTableWithBucket(tableWithFormat(GlueConstants.TABLE_FORMAT_ICEBERG), null);

    assertBucketPartition(created, 16, "id");
  }

  @Test
  void testBucketIsPartitioningWhenTheCatalogDefaultFormatIsIceberg() throws Exception {
    // The common mixed-catalog setup: the catalog defaults to Iceberg and tables inherit it.
    CreatedTable created = createTableWithBucket(ImmutableMap.of(), "iceberg");

    assertBucketPartition(created, 16, "id");
  }

  @Test
  void testBucketIsPartitioningWhenTheTableUsesTheIcebergProvider() throws Exception {
    // USING iceberg reaches the catalog as a `provider` property, which the properties converter
    // turns into table-format; the format hook has to see the converted properties.
    CreatedTable created =
        createTableWithBucket(ImmutableMap.of(TableCatalog.PROP_PROVIDER, "iceberg"), null);

    assertBucketPartition(created, 16, "id");
  }

  @Test
  void testBucketStaysBucketingForAHiveFormatTable() throws Exception {
    // With neither a table format nor a catalog default the table is not in Iceberg format, so
    // the bucket stays what it has always been for Glue tables.
    CreatedTable created = createTableWithBucket(ImmutableMap.of(), null);

    assertHashBucketing(created, 16, "id");
    Assertions.assertEquals(0, created.sortOrders.length, "a plain bucket carries no sort order");
  }

  @Test
  void testMultiColumnBucketStaysBucketingForAHiveFormatTable() throws Exception {
    // CLUSTERED BY (a, b) reaches the catalog as one bucket over two columns. Iceberg rejects
    // that; on a Hive-format table it stays bucketing over both columns, as it always was.
    CreatedTable created =
        createThroughCatalog(
            new Transform[] {Expressions.bucket(16, new String[] {"a", "b"})},
            ImmutableMap.of(),
            null);

    Assertions.assertEquals(0, created.partitioning.length, "no partitioning may be sent");
    Assertions.assertEquals(Strategy.HASH, created.distribution.strategy());
    Assertions.assertEquals(16, created.distribution.number());
    Assertions.assertEquals(2, created.distribution.expressions().length);
    Assertions.assertArrayEquals(
        new String[] {"a"},
        ((org.apache.gravitino.rel.expressions.NamedReference)
                created.distribution.expressions()[0])
            .fieldName());
    Assertions.assertArrayEquals(
        new String[] {"b"},
        ((org.apache.gravitino.rel.expressions.NamedReference)
                created.distribution.expressions()[1])
            .fieldName());
    Assertions.assertEquals(0, created.sortOrders.length, "no sort order may be sent");
  }

  @Test
  void testExplicitHiveTableFormatBeatsAnIcebergCatalogDefault() throws Exception {
    // The server rejects bucket partitioning for a Hive-format table, so the table's own format
    // has to win over the catalog default.
    CreatedTable created = createTableWithBucket(tableWithFormat("hive"), "iceberg");

    assertHashBucketing(created, 16, "id");
    Assertions.assertEquals(0, created.sortOrders.length, "a plain bucket carries no sort order");
  }

  @Test
  void testSortedBucketIsRejectedForAnIcebergFormatTable() throws Exception {
    // Iceberg has no sorted bucket. As a distribution the server dropped the bucket and kept
    // only the sort order, leaving a table the user asked to bucket unbucketed; it now fails at
    // the create, before anything is sent to Gravitino.
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                createThroughCatalog(
                    new Transform[] {sortedBucket("id", "ts")},
                    tableWithFormat(GlueConstants.TABLE_FORMAT_ICEBERG),
                    null));

    Assertions.assertTrue(error.getMessage().contains("SortedBucketTransform"), error.getMessage());
  }

  @Test
  void testSortedBucketStaysSortedBucketingForAHiveFormatTable() throws Exception {
    CreatedTable created =
        createThroughCatalog(new Transform[] {sortedBucket("id", "ts")}, ImmutableMap.of(), null);

    assertHashBucketing(created, 16, "id");
    Assertions.assertEquals(1, created.sortOrders.length, "the sort order must be kept");
    Assertions.assertArrayEquals(
        new String[] {"ts"}, created.sortOrders[0].expression().references()[0].fieldName());
  }

  @Test
  void testMultiColumnBucketIsForwardedForAnIcebergFormatTable() throws Exception {
    // Iceberg buckets take exactly one column; the server's GlueIcebergTableHelper rejects more.
    // The connector used to send such a bucket as a distribution, which the server drops for an
    // Iceberg table, creating an unpartitioned table; it now sends it as partitioning, which the
    // server rejects.
    CreatedTable created =
        createThroughCatalog(
            new Transform[] {Expressions.bucket(16, new String[] {"a", "b"})},
            tableWithFormat(GlueConstants.TABLE_FORMAT_ICEBERG),
            null);

    Assertions.assertEquals(1, created.partitioning.length);
    Transforms.BucketTransform bucket =
        Assertions.assertInstanceOf(Transforms.BucketTransform.class, created.partitioning[0]);
    Assertions.assertArrayEquals(new String[][] {{"a"}, {"b"}}, bucket.fieldNames());
    Assertions.assertEquals(16, bucket.numBuckets());
    Assertions.assertEquals(Strategy.NONE, created.distribution.strategy());
    Assertions.assertEquals(0, created.sortOrders.length, "a plain bucket carries no sort order");
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

  private static Transform sortedBucket(String bucketColumn, String sortColumn) {
    return LogicalExpressions.bucket(
        16,
        new NamedReference[] {FieldReference.apply(bucketColumn)},
        new NamedReference[] {FieldReference.apply(sortColumn)});
  }

  private static Map<String, String> tableWithFormat(String format) {
    return ImmutableMap.of(GlueConstants.TABLE_FORMAT, format);
  }

  /** Creates a GravitinoGlueCatalog with the given Iceberg catalog pre-injected. */
  private GravitinoGlueCatalog makeGlueCatalogWithIceberg(SparkCatalog icebergCatalog) {
    return new GravitinoGlueCatalog() {
      {
        icebergGlueCatalog = icebergCatalog;
      }
    };
  }

  /** Asserts the table was sent with one bucket partition over {@code column} and no bucketing. */
  private static void assertBucketPartition(CreatedTable created, int numBuckets, String column) {
    Assertions.assertEquals(1, created.partitioning.length, "bucket must become the partitioning");
    Transforms.BucketTransform bucket =
        Assertions.assertInstanceOf(
            Transforms.BucketTransform.class, created.partitioning[0], "partitioning transform");
    Assertions.assertEquals(numBuckets, bucket.numBuckets());
    Assertions.assertArrayEquals(new String[][] {{column}}, bucket.fieldNames());
    Assertions.assertEquals(
        Strategy.NONE,
        created.distribution.strategy(),
        "the bucket must not also be sent as a distribution");
    Assertions.assertEquals(0, created.sortOrders.length, "a plain bucket carries no sort order");
  }

  /** Asserts the table was sent with no partitioning and one hash bucketing over {@code column}. */
  private static void assertHashBucketing(CreatedTable created, int numBuckets, String column) {
    Assertions.assertEquals(0, created.partitioning.length, "no partitioning may be sent");
    Assertions.assertEquals(Strategy.HASH, created.distribution.strategy());
    Assertions.assertEquals(numBuckets, created.distribution.number());
    Assertions.assertEquals(
        1,
        created.distribution.expressions().length,
        "the bucketing must cover exactly one column");
    Assertions.assertArrayEquals(
        new String[] {column},
        ((org.apache.gravitino.rel.expressions.NamedReference)
                created.distribution.expressions()[0])
            .fieldName());
  }

  /** What the catalog handed to Gravitino's createTable for one table. */
  private static final class CreatedTable {

    private final org.apache.gravitino.rel.expressions.transforms.Transform[] partitioning;
    private final Distribution distribution;
    private final SortOrder[] sortOrders;

    private CreatedTable(
        org.apache.gravitino.rel.expressions.transforms.Transform[] partitioning,
        Distribution distribution,
        SortOrder[] sortOrders) {
      this.partitioning = partitioning;
      this.distribution = distribution;
      this.sortOrders = sortOrders;
    }
  }

  /**
   * Creates a table with a single {@code bucket(16, id)} transform. See {@link
   * #createThroughCatalog}.
   */
  private CreatedTable createTableWithBucket(
      Map<String, String> tableProperties, @Nullable String catalogDefaultTableFormat)
      throws Exception {
    return createThroughCatalog(
        new Transform[] {Expressions.bucket(16, new String[] {"id"})},
        tableProperties,
        catalogDefaultTableFormat);
  }

  /**
   * Creates a table with the given transforms and returns the partitioning, distribution and sort
   * orders the catalog sent to Gravitino.
   *
   * @param transforms the Spark transforms of the CREATE TABLE
   * @param tableProperties the Spark table properties of the CREATE TABLE; the catalog converts
   *     them to Gravitino properties before its format hooks see them
   * @param catalogDefaultTableFormat the catalog's {@code default-table-format}, or null when the
   *     catalog sets none
   */
  private CreatedTable createThroughCatalog(
      Transform[] transforms,
      Map<String, String> tableProperties,
      @Nullable String catalogDefaultTableFormat)
      throws Exception {
    org.apache.gravitino.rel.TableCatalog gravitinoTableCatalog =
        mock(org.apache.gravitino.rel.TableCatalog.class);
    Catalog client = mock(Catalog.class);
    when(client.asTableCatalog()).thenReturn(gravitinoTableCatalog);

    TestableGlueCatalog catalog = new TestableGlueCatalog(client);
    // initialize() normally fills these two: catalogProperties from the Gravitino catalog, the
    // type converter from the catalog's hook. Setting them directly skips initialize(), not the
    // BaseCatalog constructor: that still needs the catalog manager this class creates in
    // @BeforeAll.
    setField(
        catalog,
        GravitinoGlueCatalog.class,
        "catalogProperties",
        catalogDefaultTableFormat == null
            ? ImmutableMap.of()
            : ImmutableMap.of(GlueConstants.DEFAULT_TABLE_FORMAT, catalogDefaultTableFormat));
    setField(catalog, BaseCatalog.class, "sparkTypeConverter", catalog.defaultTypeConverter());

    catalog.createTable(transforms, tableProperties);

    ArgumentCaptor<org.apache.gravitino.rel.expressions.transforms.Transform[]> partitioning =
        ArgumentCaptor.forClass(org.apache.gravitino.rel.expressions.transforms.Transform[].class);
    ArgumentCaptor<Distribution> distribution = ArgumentCaptor.forClass(Distribution.class);
    ArgumentCaptor<SortOrder[]> sortOrders = ArgumentCaptor.forClass(SortOrder[].class);
    verify(gravitinoTableCatalog)
        .createTable(
            any(),
            any(),
            any(),
            any(),
            partitioning.capture(),
            distribution.capture(),
            sortOrders.capture());
    return new CreatedTable(
        partitioning.getValue(), distribution.getValue(), sortOrders.getValue());
  }

  private static void setField(Object target, Class<?> declaringClass, String name, Object value)
      throws Exception {
    Field field = declaringClass.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  /** A Glue catalog over a mocked Gravitino client, exposing the protected create path. */
  private static class TestableGlueCatalog extends GravitinoGlueCatalog {

    private final Catalog client;

    private TestableGlueCatalog(Catalog client) {
      this.client = client;
      this.propertiesConverter = GluePropertiesConverter.getInstance();
      // initialize() stores this catalog-wide converter as well; a create path that reads this
      // field instead of the per-table hook would otherwise fail here with an NPE.
      this.sparkTransformConverter = getSparkTransformConverter();
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return client;
    }

    /** The type converter initialize() would store for a Glue catalog. */
    private SparkTypeConverter defaultTypeConverter() {
      return getSparkTypeConverter();
    }

    private void createTable(Transform[] transforms, Map<String, String> tableProperties)
        throws Exception {
      createGravitinoTable(
          Identifier.of(new String[] {"db"}, "t"),
          new StructType()
              .add("id", DataTypes.IntegerType)
              .add("ts", DataTypes.TimestampType)
              .add("a", DataTypes.IntegerType)
              .add("b", DataTypes.IntegerType),
          transforms,
          tableProperties);
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
