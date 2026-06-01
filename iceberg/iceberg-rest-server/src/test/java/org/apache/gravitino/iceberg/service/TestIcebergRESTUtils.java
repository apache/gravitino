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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
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
    Assertions.assertEquals(
        "v1/my_catalog/namespaces/ns/tables/tbl/credentials",
        IcebergRESTUtils.tableCredentialsPath("my_catalog", table));
  }

  @Test
  void testToRestCredential() {
    TableIdentifier table = TableIdentifier.of(Namespace.of("ns"), "tbl");
    TableMetadata metadataWithSlash = mock(TableMetadata.class);
    when(metadataWithSlash.location()).thenReturn("s3://bucket/t/");
    org.apache.iceberg.rest.credentials.Credential credentialWithSlash =
        IcebergRESTUtils.toRestCredential(
            "cat", table, new S3TokenCredential("k", "s", "t", 99L), metadataWithSlash);
    Assertions.assertEquals("s3://bucket/t/", credentialWithSlash.prefix());
    Assertions.assertEquals(
        "99", credentialWithSlash.config().get("s3.session-token-expires-at-ms"));

    TableMetadata metadataWithoutSlash = mock(TableMetadata.class);
    when(metadataWithoutSlash.location()).thenReturn("s3://bucket/path/to/table");
    org.apache.iceberg.rest.credentials.Credential credentialWithoutSlash =
        IcebergRESTUtils.toRestCredential(
            "cat", table, new S3TokenCredential("k", "s", "t", 99L), metadataWithoutSlash);
    Assertions.assertEquals("s3://bucket/path/to/table/", credentialWithoutSlash.prefix());
  }

  @Test
  void testAppendRefreshEndpointForS3Token() {
    Map<String, String> config =
        new HashMap<>(
            CredentialPropertyUtils.toIcebergProperties(
                new S3TokenCredential("key", "secret", "token", 1234L)));
    IcebergRESTUtils.appendRefreshEndpoint(
        config,
        new S3TokenCredential("key", "secret", "token", 1234L),
        "v1/aws/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergRESTUtils.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForGcsToken() {
    Credential gcsToken = new GCSTokenCredential("gcs-token", 5678L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(gcsToken));
    IcebergRESTUtils.appendRefreshEndpoint(
        config, gcsToken, "v1/gcs/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/gcs/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergRESTUtils.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForOssToken() {
    Credential ossToken = new OSSTokenCredential("key", "secret", "oss-token", 9012L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(ossToken));
    IcebergRESTUtils.appendRefreshEndpoint(
        config, ossToken, "v1/oss/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/oss/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergRESTUtils.OSS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForAdlsToken() {
    Credential adlsToken = new ADLSTokenCredential("storageacct", "sas-token", 3456L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(adlsToken));
    IcebergRESTUtils.appendRefreshEndpoint(
        config, adlsToken, "v1/adls/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/adls/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergRESTUtils.ADLS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointOmitsStaticSecretKey() {
    Map<String, String> config =
        new HashMap<>(
            CredentialPropertyUtils.toIcebergProperties(
                new S3SecretKeyCredential("key", "secret")));
    IcebergRESTUtils.appendRefreshEndpoint(
        config,
        new S3SecretKeyCredential("key", "secret"),
        "v1/aws/namespaces/ns/tables/tbl/credentials");

    Assertions.assertFalse(config.containsKey(IcebergRESTUtils.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }
}
