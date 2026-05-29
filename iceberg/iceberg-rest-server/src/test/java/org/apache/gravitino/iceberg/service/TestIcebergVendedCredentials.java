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
package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergVendedCredentials {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("ns"), "tbl");

  @Test
  void testTableCredentialsPath() {
    Assertions.assertEquals(
        "v1/my_catalog/namespaces/ns/tables/tbl/credentials",
        IcebergVendedCredentials.tableCredentialsPath("my_catalog", TABLE));
  }

  @Test
  void testS3TokenClientConfig() {
    Map<String, String> config =
        vendedConfig("aws", new S3TokenCredential("key", "secret", "token", 1234L));

    Assertions.assertEquals("key", config.get(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    Assertions.assertEquals("1234", config.get(IcebergVendedCredentials.S3_TOKEN_EXPIRES_AT_MS));
    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergVendedCredentials.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testGcsTokenClientConfig() {
    Map<String, String> config = vendedConfig("gcs", new GCSTokenCredential("gcs-token", 5678L));

    Assertions.assertEquals("gcs-token", config.get(IcebergConstants.GCS_OAUTH2_TOKEN));
    Assertions.assertEquals("5678", config.get(IcebergConstants.GCS_OAUTH2_TOKEN_EXPIRES_AT));
    Assertions.assertEquals(
        "v1/gcs/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergVendedCredentials.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testOssTokenClientConfig() {
    Map<String, String> config =
        vendedConfig("oss", new OSSTokenCredential("key", "secret", "oss-token", 9012L));

    Assertions.assertEquals(
        "9012", config.get(IcebergVendedCredentials.OSS_SECURITY_TOKEN_EXPIRES_AT_MS));
    Assertions.assertEquals(
        "v1/oss/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergVendedCredentials.OSS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAdlsTokenClientConfig() {
    Map<String, String> config =
        vendedConfig("adls", new ADLSTokenCredential("storageacct", "sas-token", 3456L));

    Assertions.assertEquals(
        "sas-token", config.get("adls.sas-token.storageacct.dfs.core.windows.net"));
    Assertions.assertEquals(
        "3456",
        config.get(IcebergVendedCredentials.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + "storageacct"));
    Assertions.assertEquals(
        "v1/adls/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergVendedCredentials.ADLS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testStaticSecretKeyOmitsRefreshEndpoint() {
    Map<String, String> config = vendedConfig("aws", new S3SecretKeyCredential("key", "secret"));

    Assertions.assertFalse(
        config.containsKey(IcebergVendedCredentials.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testToRestCredential() {
    TableMetadata metadataWithSlash = mock(TableMetadata.class);
    when(metadataWithSlash.location()).thenReturn("s3://bucket/t/");
    org.apache.iceberg.rest.credentials.Credential credentialWithSlash =
        IcebergVendedCredentials.toRestCredential(
            "cat", TABLE, new S3TokenCredential("k", "s", "t", 99L), metadataWithSlash);
    Assertions.assertEquals("s3://bucket/t/", credentialWithSlash.prefix());
    Assertions.assertEquals(
        "99", credentialWithSlash.config().get(IcebergVendedCredentials.S3_TOKEN_EXPIRES_AT_MS));

    TableMetadata metadataWithoutSlash = mock(TableMetadata.class);
    when(metadataWithoutSlash.location()).thenReturn("s3://bucket/path/to/table");
    org.apache.iceberg.rest.credentials.Credential credentialWithoutSlash =
        IcebergVendedCredentials.toRestCredential(
            "cat", TABLE, new S3TokenCredential("k", "s", "t", 99L), metadataWithoutSlash);
    Assertions.assertEquals("s3://bucket/path/to/table/", credentialWithoutSlash.prefix());
  }

  private static Map<String, String> vendedConfig(String catalogName, Credential credential) {
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(credential));
    IcebergVendedCredentials.appendVendedRefreshProperties(config, catalogName, TABLE, credential);
    return config;
  }
}
