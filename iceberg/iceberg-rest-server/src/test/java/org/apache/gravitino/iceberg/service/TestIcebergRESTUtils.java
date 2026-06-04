/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestIcebergRESTUtils {

  @BeforeAll
  public void init() {
    IcebergConfigProvider icebergConfigProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(icebergConfigProvider.getMetalakeName()).thenReturn("metalake");
    Mockito.when(icebergConfigProvider.getDefaultCatalogName())
        .thenReturn(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG);
    IcebergRESTServerContext.create(icebergConfigProvider, false, false, true, null);
  }

  @Test
  void testGetGravitinoNameIdentifier() {
    String metalakeName = "metalake";
    String catalogName = "catalog";
    TableIdentifier tableIdentifier = TableIdentifier.of("ns1", "ns2", "table");
    NameIdentifier nameIdentifier =
        IcebergRESTUtils.getGravitinoNameIdentifier(metalakeName, catalogName, tableIdentifier);
    Assertions.assertEquals(
        NameIdentifier.of(metalakeName, catalogName, "ns1", "ns2", "table"), nameIdentifier);
  }

  @Test
  void testGetCatalogName() {
    String prefix = "catalog/";
    Assertions.assertEquals("catalog", IcebergRESTUtils.getCatalogName(prefix));
    Assertions.assertEquals(
        IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG, IcebergRESTUtils.getCatalogName(""));
  }

  @Test
  void testSerdeIcebergRESTObject() {
    Schema tableSchema =
        new Schema(
            NestedField.of(1, false, "foo1", StringType.get()),
            NestedField.of(2, true, "foo2", IntegerType.get()));
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder().withName("table").withSchema(tableSchema).build();
    CreateTableRequest clonedIcebergRESTObject =
        IcebergRESTUtils.cloneIcebergRESTObject(createTableRequest, CreateTableRequest.class);
    Assertions.assertEquals(createTableRequest.name(), clonedIcebergRESTObject.name());
    Assertions.assertEquals(
        createTableRequest.schema().columns().size(),
        clonedIcebergRESTObject.schema().columns().size());
    for (int i = 0; i < createTableRequest.schema().columns().size(); i++) {
      NestedField field = createTableRequest.schema().columns().get(i);
      NestedField clonedField = clonedIcebergRESTObject.schema().columns().get(i);
      Assertions.assertEquals(field, clonedField);
    }
  }

  @Test
  void testTableCredentialsPath() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("s3://bucket/t/");
    org.apache.iceberg.rest.credentials.Credential credential =
        IcebergRESTUtils.toRESTCredential(
            "my_catalog", table, new S3TokenCredential("k", "s", "t", 99L), tableMetadata);
    Assertions.assertEquals(
        "v1/my_catalog/namespaces/ns/tables/tbl/credentials",
        credential.config().get("client.refresh-credentials-endpoint"));
  }

  @Test
  void testToRESTCredential() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    String refreshPath = "v1/cat/namespaces/ns/tables/tbl/credentials";
    TableMetadata metadataWithSlash = mock(TableMetadata.class);
    when(metadataWithSlash.location()).thenReturn("s3://bucket/t/");
    org.apache.iceberg.rest.credentials.Credential credentialWithSlash =
        IcebergRESTUtils.toRESTCredential(
            "cat", table, new S3TokenCredential("k", "s", "t", 99L), metadataWithSlash);
    Assertions.assertEquals("s3://bucket/t/", credentialWithSlash.prefix());
    Assertions.assertEquals(
        "99", credentialWithSlash.config().get("s3.session-token-expires-at-ms"));
    Assertions.assertEquals(
        refreshPath, credentialWithSlash.config().get("client.refresh-credentials-endpoint"));

    TableMetadata metadataWithoutSlash = mock(TableMetadata.class);
    when(metadataWithoutSlash.location()).thenReturn("s3://bucket/path/to/table");
    org.apache.iceberg.rest.credentials.Credential credentialWithoutSlash =
        IcebergRESTUtils.toRESTCredential(
            "cat", table, new S3TokenCredential("k", "s", "t", 99L), metadataWithoutSlash);
    Assertions.assertEquals("s3://bucket/path/to/table", credentialWithoutSlash.prefix());
  }

  @Test
  void testToRESTCredentialForS3Token() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("s3://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "aws", table, new S3TokenCredential("key", "secret", "token", 1234L), tableMetadata)
            .config();

    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get("client.refresh-credentials-endpoint"));
    Assertions.assertEquals("1234", config.get("s3.session-token-expires-at-ms"));
  }

  @Test
  void testToRESTCredentialForAwsIrsa() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("s3://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "aws", table, new AwsIrsaCredential("key", "secret", "token", 4321L), tableMetadata)
            .config();

    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get("client.refresh-credentials-endpoint"));
    Assertions.assertEquals("4321", config.get("s3.session-token-expires-at-ms"));
  }

  @Test
  void testToRESTCredentialConfigIsUnmodifiable() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("s3://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "aws", table, new S3TokenCredential("k", "s", "t", 99L), tableMetadata)
            .config();

    Assertions.assertThrows(UnsupportedOperationException.class, () -> config.put("k", "v"));
  }

  @Test
  void testToRESTCredentialForGcsToken() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("gs://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "gcs", table, new GCSTokenCredential("gcs-token", 5678L), tableMetadata)
            .config();

    Assertions.assertEquals(
        "v1/gcs/namespaces/ns/tables/tbl/credentials",
        config.get("gcs.oauth2.refresh-credentials-endpoint"));
    Assertions.assertEquals("5678", config.get("gcs.oauth2.token-expires-at"));
  }

  @Test
  void testToRESTCredentialForOssToken() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("oss://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "oss",
                table,
                new OSSTokenCredential("key", "secret", "oss-token", 9012L),
                tableMetadata)
            .config();

    Assertions.assertFalse(config.containsKey("client.refresh-credentials-endpoint"));
    Assertions.assertEquals("9012", config.get("client.security-token-expires-at-ms"));
    Assertions.assertEquals("key", config.get("client.access-key-id"));
    Assertions.assertEquals("secret", config.get("client.access-key-secret"));
    Assertions.assertEquals("oss-token", config.get("client.security-token"));
  }

  @Test
  void testToRESTCredentialForAdlsToken() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("abfss://container@account.dfs.core.windows.net/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "adls",
                table,
                new ADLSTokenCredential("storageacct", "sas-token", 3456L),
                tableMetadata)
            .config();

    Assertions.assertEquals(
        "v1/adls/namespaces/ns/tables/tbl/credentials",
        config.get("adls.refresh-credentials-endpoint"));
    Assertions.assertEquals(
        "3456", config.get("adls.sas-token-expires-at-ms.storageacct.dfs.core.windows.net"));
  }

  @Test
  void testBuildStorageCreds() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("db"), "tbl");
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    SupportsStorageCredentials storageCredentialsFileIO = (SupportsStorageCredentials) fileIO;
    StorageCredential upstreamCredential =
        StorageCredential.create(
            "s3://bucket/db/tbl/",
            ImmutableMap.of(
                "s3.access-key-id",
                "upstream-key",
                "s3.secret-access-key",
                "upstream-secret",
                "s3.session-token",
                "upstream-token",
                "s3.session-token-expires-at-ms",
                "123",
                "client.refresh-credentials-endpoint",
                "v1/upstream/namespaces/db/tables/tbl/credentials"));
    when(storageCredentialsFileIO.credentials()).thenReturn(List.of(upstreamCredential));

    List<Credential> credentials = IcebergRESTUtils.buildStorageCreds("irc1", table, fileIO);

    Assertions.assertEquals(1, credentials.size());
    Credential credential = credentials.get(0);
    Assertions.assertEquals("s3://bucket/db/tbl/", credential.prefix());
    Assertions.assertEquals("upstream-token", credential.config().get("s3.session-token"));
    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        credential.config().get("client.refresh-credentials-endpoint"));
    Assertions.assertFalse(
        credential.config().containsValue("v1/upstream/namespaces/db/tables/tbl/credentials"));
  }

  @Test
  void testBuildStorageCredsPreservesSchemePrefix() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("db"), "tbl");
    FileIO fileIO =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    SupportsStorageCredentials storageCredentialsFileIO = (SupportsStorageCredentials) fileIO;
    StorageCredential upstreamCredential =
        StorageCredential.create(
            "s3",
            ImmutableMap.of(
                "s3.session-token", "upstream-token",
                "s3.session-token-expires-at-ms", "123"));
    when(storageCredentialsFileIO.credentials()).thenReturn(List.of(upstreamCredential));

    Credential credential = IcebergRESTUtils.buildStorageCreds("irc1", table, fileIO).get(0);

    Assertions.assertEquals("s3", credential.prefix());
  }

  @Test
  void testBuildStorageCredsEmpty() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("db"), "tbl");
    FileIO fileIO = mock(FileIO.class);

    Assertions.assertTrue(IcebergRESTUtils.buildStorageCreds("irc1", table, fileIO).isEmpty());
  }

  @Test
  void testToRESTCredentialOmitsStaticSecretKey() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata tableMetadata = mock(TableMetadata.class);
    when(tableMetadata.location()).thenReturn("s3://bucket/t/");
    Map<String, String> config =
        IcebergRESTUtils.toRESTCredential(
                "aws", table, new S3SecretKeyCredential("key", "secret"), tableMetadata)
            .config();

    Assertions.assertFalse(config.containsKey("client.refresh-credentials-endpoint"));
  }
}
