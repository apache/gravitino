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

import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.ADLSTokenCredential;
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
  void testTableLocationPrefix() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("s3://bucket/path/to/table");

    Assertions.assertEquals(
        "s3://bucket/path/to/table/", IcebergVendedCredentials.tableLocationPrefix(metadata));
  }

  @Test
  void testS3TokenClientConfig() {
    Map<String, String> config =
        IcebergVendedCredentials.toClientConfig(
            "aws", TABLE, new S3TokenCredential("key", "secret", "token", 1234L));

    Assertions.assertEquals("key", config.get(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID));
    Assertions.assertEquals("1234", config.get(IcebergConstants.ICEBERG_S3_TOKEN_EXPIRES_AT_MS));
    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergConstants.CLIENT_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testGcsTokenClientConfig() {
    Map<String, String> config =
        IcebergVendedCredentials.toClientConfig(
            "gcs", TABLE, new GCSTokenCredential("gcs-token", 5678L));

    Assertions.assertEquals("gcs-token", config.get("gcs.oauth2.token"));
    Assertions.assertEquals("5678", config.get(IcebergConstants.GCS_OAUTH2_TOKEN_EXPIRES_AT));
    Assertions.assertEquals(
        "v1/gcs/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergConstants.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testOssTokenClientConfig() {
    Map<String, String> config =
        IcebergVendedCredentials.toClientConfig(
            "oss", TABLE, new OSSTokenCredential("key", "secret", "oss-token", 9012L));

    Assertions.assertEquals(
        "9012", config.get(IcebergConstants.ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS));
    Assertions.assertEquals(
        "v1/oss/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergConstants.CLIENT_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAdlsTokenClientConfig() {
    Map<String, String> config =
        IcebergVendedCredentials.toClientConfig(
            "adls", TABLE, new ADLSTokenCredential("storageacct", "sas-token", 3456L));

    Assertions.assertEquals(
        "sas-token", config.get("adls.sas-token.storageacct.dfs.core.windows.net"));
    Assertions.assertEquals("3456", config.get("adls.sas-token-expires-at-ms.storageacct"));
    Assertions.assertEquals(
        "v1/adls/namespaces/ns/tables/tbl/credentials",
        config.get(IcebergConstants.ADLS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testStaticSecretKeyOmitsRefreshEndpoint() {
    Map<String, String> config =
        IcebergVendedCredentials.toClientConfig(
            "aws", TABLE, new S3SecretKeyCredential("key", "secret"));

    Assertions.assertFalse(
        config.containsKey(IcebergConstants.CLIENT_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testToRestCredential() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("s3://bucket/t/");

    org.apache.iceberg.rest.credentials.Credential credential =
        IcebergVendedCredentials.toRestCredential(
            "cat", TABLE, new S3TokenCredential("k", "s", "t", 99L), metadata);

    Assertions.assertEquals("s3://bucket/t/", credential.prefix());
    Assertions.assertEquals(
        "99", credential.config().get(IcebergConstants.ICEBERG_S3_TOKEN_EXPIRES_AT_MS));
  }
}
