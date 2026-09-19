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

package org.apache.gravitino.lakehouse.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesUtils {

  @Test
  void testJdbcSchemaVersionPropertyIsMapped() {
    Map<String, String> gravitinoProps =
        ImmutableMap.of(IcebergConstants.GRAVITINO_JDBC_SCHEMA_VERSION, "V1");
    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProps);
    Assertions.assertEquals(
        "V1",
        icebergProps.get(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION),
        "jdbc-schema-version must be translated to jdbc.schema-version for Iceberg");
  }

  @Test
  void testRESTCatalogBackendClientTimeoutPropertiesAreMapped() {
    Map<String, String> gravitinoProps =
        ImmutableMap.of(
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_CONNECTION_TIMEOUT_MS,
            "1000",
            IcebergConstants.REST_CATALOG_BACKEND_CLIENT_SOCKET_TIMEOUT_MS,
            "2000");
    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProps);

    Assertions.assertEquals(
        "1000", icebergProps.get(IcebergConstants.ICEBERG_REST_CLIENT_CONNECTION_TIMEOUT_MS));
    Assertions.assertEquals(
        "2000", icebergProps.get(IcebergConstants.ICEBERG_REST_CLIENT_SOCKET_TIMEOUT_MS));
  }

  @Test
  void testAzureServicePrincipalPropertiesAreMapped() {
    Map<String, String> gravitinoProps =
        ImmutableMap.of(
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
            "account",
            AzureProperties.GRAVITINO_AZURE_TENANT_ID,
            "tenant",
            AzureProperties.GRAVITINO_AZURE_CLIENT_ID,
            "client",
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
            "secret");

    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProps);

    Assertions.assertFalse(
        icebergProps.containsKey(IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_NAME));
    Assertions.assertFalse(
        icebergProps.containsKey(IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_KEY));
    Assertions.assertEquals(
        IcebergConstants.AZURE_CLIENT_SECRET_TOKEN_CREDENTIAL_PROVIDER,
        icebergProps.get(IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER));
    Assertions.assertEquals(
        "tenant",
        icebergProps.get(
            IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX
                + AzureProperties.GRAVITINO_AZURE_TENANT_ID));
    Assertions.assertEquals(
        "client",
        icebergProps.get(
            IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX
                + AzureProperties.GRAVITINO_AZURE_CLIENT_ID));
    Assertions.assertEquals(
        "secret",
        icebergProps.get(
            IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX
                + AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));
  }

  @Test
  void testAzureSharedKeyTakesPrecedenceOverServicePrincipal() {
    Map<String, String> gravitinoProps =
        ImmutableMap.of(
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
            "account",
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
            "account-key",
            AzureProperties.GRAVITINO_AZURE_TENANT_ID,
            "tenant",
            AzureProperties.GRAVITINO_AZURE_CLIENT_ID,
            "client",
            AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
            "secret");

    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProps);

    Assertions.assertEquals(
        "account", icebergProps.get(IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_NAME));
    Assertions.assertEquals(
        "account-key", icebergProps.get(IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_KEY));
    Assertions.assertFalse(
        icebergProps.containsKey(IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER));
    Assertions.assertFalse(
        icebergProps.containsKey(
            IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX
                + AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET));
  }

  @Test
  void testIncompleteAzureServicePrincipalPreservesSharedKeyValidation() {
    Map<String, String> gravitinoProps =
        ImmutableMap.of(
            AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
            "account",
            AzureProperties.GRAVITINO_AZURE_TENANT_ID,
            "tenant");

    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(gravitinoProps);

    Assertions.assertEquals(
        "account", icebergProps.get(IcebergConstants.ICEBERG_ADLS_STORAGE_ACCOUNT_NAME));
    Assertions.assertFalse(
        icebergProps.containsKey(IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER));
    Assertions.assertFalse(
        icebergProps.containsKey(
            IcebergConstants.ICEBERG_ADLS_TOKEN_CREDENTIAL_PROVIDER_PREFIX
                + AzureProperties.GRAVITINO_AZURE_TENANT_ID));
  }

  @Test
  void testGetCatalogBackendName() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            IcebergConstants.CATALOG_BACKEND_NAME, "a", IcebergConstants.CATALOG_BACKEND, "jdbc");
    String backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("a", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "jdbc");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("jdbc", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "JDBC");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("jdbc", backendName);

    catalogProperties = ImmutableMap.of(IcebergConstants.CATALOG_BACKEND, "hive");
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("hive", backendName);

    catalogProperties = ImmutableMap.of();
    backendName = IcebergPropertiesUtils.getCatalogBackendName(catalogProperties);
    Assertions.assertEquals("memory", backendName);
  }

  @Test
  void testTableFormatVersionFallbacks() {
    Map<String, String> unset = ImmutableMap.of();
    Assertions.assertFalse(
        IcebergPropertiesUtils.configuredDefaultTableFormatVersion(unset).isPresent());
    Assertions.assertEquals(
        IcebergConstants.DEFAULT_TABLE_FORMAT_VERSION,
        IcebergPropertiesUtils.defaultTableFormatVersion(unset));
    Assertions.assertEquals(
        IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION,
        IcebergPropertiesUtils.maxTableFormatVersion(unset));
    Assertions.assertEquals(4, IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION);
    Assertions.assertDoesNotThrow(() -> IcebergPropertiesUtils.validateTableFormatVersions(unset));

    Map<String, String> set =
        ImmutableMap.of(
            IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
            " 3 ",
            IcebergConstants.TABLE_FORMAT_VERSION_MAX,
            "3");
    Assertions.assertEquals(3, IcebergPropertiesUtils.defaultTableFormatVersion(set));
    Assertions.assertEquals(3, IcebergPropertiesUtils.maxTableFormatVersion(set));
  }

  @Test
  void testParseTableFormatVersionAcceptsOnlySupportedVersions() {
    for (int version : IcebergConstants.SUPPORTED_TABLE_FORMAT_VERSIONS) {
      Assertions.assertEquals(
          version, IcebergPropertiesUtils.parseTableFormatVersion("p", String.valueOf(version)));
    }
    for (String invalid : new String[] {"0", "5", "-1", "2.0", "abc", "", null}) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> IcebergPropertiesUtils.parseTableFormatVersion("p", invalid),
              String.valueOf(invalid));
      Assertions.assertTrue(e.getMessage().contains("'p'"), e.getMessage());
    }
  }

  @Test
  void testCheckTableFormatVersionAllowed() {
    Assertions.assertDoesNotThrow(
        () -> IcebergPropertiesUtils.checkTableFormatVersionAllowed(3, 3));
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergPropertiesUtils.checkTableFormatVersionAllowed(4, 3));
    Assertions.assertTrue(
        e.getMessage().contains(IcebergConstants.TABLE_FORMAT_VERSION_MAX), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("limit 3"), e.getMessage());
  }

  /**
   * Above the build's ceiling the error names the supported range, not the catalog maximum, both
   * when the maximum is unset (it equals the ceiling) and when an operator set a lower one.
   */
  @Test
  void testCheckTableFormatVersionAllowedNamesTheBuildCeiling() {
    int ceiling = IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION;
    Assertions.assertDoesNotThrow(
        () -> IcebergPropertiesUtils.checkTableFormatVersionAllowed(ceiling, ceiling));
    for (int max : new int[] {ceiling, 2}) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> IcebergPropertiesUtils.checkTableFormatVersionAllowed(ceiling + 1, max));
      Assertions.assertEquals(
          "Iceberg format-version 5 is not supported by this Gravitino (supports 1-4)",
          e.getMessage());
    }
  }

  @Test
  void testTableFormatVersionPropertiesAreMappedToIcebergConfig() {
    Map<String, String> icebergProps =
        IcebergPropertiesUtils.toIcebergCatalogProperties(
            ImmutableMap.of(
                IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT,
                "3",
                IcebergConstants.TABLE_FORMAT_VERSION_MAX,
                "4"));
    Assertions.assertEquals("3", icebergProps.get(IcebergConstants.TABLE_FORMAT_VERSION_DEFAULT));
    Assertions.assertEquals("4", icebergProps.get(IcebergConstants.TABLE_FORMAT_VERSION_MAX));
  }
}
