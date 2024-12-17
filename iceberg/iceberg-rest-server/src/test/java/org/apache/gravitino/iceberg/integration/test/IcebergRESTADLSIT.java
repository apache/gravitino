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

package org.apache.gravitino.iceberg.integration.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.abs.credential.ADLSLocationUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@SuppressWarnings("FormatStringAnnotation")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTADLSIT extends IcebergRESTJdbcCatalogIT {

  private String storageAccountName;
  private String storageAccountKey;
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private String warehousePath;

  @Override
  void initEnv() {
    this.storageAccountName =
        System.getenv()
            .getOrDefault("GRAVITINO_ADLS_STORAGE_ACCOUNT_NAME", "{STORAGE_ACCOUNT_NAME}");
    this.storageAccountKey =
        System.getenv().getOrDefault("GRAVITINO_ADLS_STORAGE_ACCOUNT_KEY", "{STORAGE_ACCOUNT_KEY}");
    this.tenantId = System.getenv().getOrDefault("GRAVITINO_ADLS_TENANT_ID", "{TENANT_ID}");
    this.clientId = System.getenv().getOrDefault("GRAVITINO_ADLS_CLIENT_ID", "{CLIENT_ID}");
    this.clientSecret =
        System.getenv().getOrDefault("GRAVITINO_ADLS_CLIENT_SECRET", "{CLIENT_SECRET}");
    this.warehousePath =
        String.format(
            "abfss://%s@%s.dfs.core.windows.net/data/test",
            System.getenv().getOrDefault("GRAVITINO_ADLS_CONTAINER", "{ADLS_CONTAINER}"),
            storageAccountName);

    if (ITUtils.isEmbedded()) {
      return;
    }
    try {
      downloadIcebergAzureBundleJar();
    } catch (IOException e) {
      LOG.warn("Download Iceberg Azure bundle jar failed,", e);
      throw new RuntimeException(e);
    }
    copyAzureBundleJar();
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    HashMap m = new HashMap<String, String>();
    m.putAll(getCatalogJdbcConfig());
    m.putAll(getADLSConfig());
    return m;
  }

  public boolean supportsCredentialVending() {
    return true;
  }

  private Map<String, String> getADLSConfig() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.ADLS_TOKEN_CREDENTIAL_PROVIDER_TYPE);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
        storageAccountName);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
        storageAccountKey);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + AzureProperties.GRAVITINO_AZURE_TENANT_ID, tenantId);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + AzureProperties.GRAVITINO_AZURE_CLIENT_ID, clientId);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
        clientSecret);

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, warehousePath);

    return configMap;
  }

  private void downloadIcebergAzureBundleJar() throws IOException {
    String icebergBundleJarName = "iceberg-azure-bundle-1.5.2.jar";
    String icebergBundleJarUri =
        "https://repo1.maven.org/maven2/org/apache/iceberg/"
            + "iceberg-azure-bundle/1.5.2/"
            + icebergBundleJarName;
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private void copyAzureBundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("azure-bundle", targetDir);
  }

  @Test
  public void testParseLocationValidInput() {
    String location = "abfss://container@account.dfs.core.windows.net/data/test/path";

    ADLSLocationUtils.ADLSLocationParts parts = ADLSLocationUtils.parseLocation(location);

    Assertions.assertEquals("container", parts.getContainer(), "Container name should match");
    Assertions.assertEquals("account", parts.getAccountName(), "Account name should match");
    Assertions.assertEquals("/data/test/path", parts.getPath(), "Path should match");
  }

  @Test
  public void testParseLocationInvalidInput() {
    String location = "abfss://container/invalid/location";

    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              ADLSLocationUtils.parseLocation(location);
            });

    Assertions.assertTrue(
        exception.getMessage().contains("Invalid location"),
        "Exception message should indicate invalid location");
  }

  @Test
  public void testTrimSlashesNullInput() {
    Assertions.assertNull(ADLSLocationUtils.trimSlashes(null), "Null input should return null");
  }

  @Test
  public void testTrimSlashesEmptyInput() {
    Assertions.assertEquals(
        "", ADLSLocationUtils.trimSlashes(""), "Empty input should return empty string");
  }

  @Test
  public void testTrimSlashesNoSlashes() {
    String input = "data/test/path";
    Assertions.assertEquals(
        "data/test/path",
        ADLSLocationUtils.trimSlashes(input),
        "Input without slashes should remain unchanged");
  }

  @Test
  public void testTrimSlashesLeadingAndTrailingSlashes() {
    String input = "/data/test/path/";
    Assertions.assertEquals(
        "data/test/path",
        ADLSLocationUtils.trimSlashes(input),
        "Leading and trailing slashes should be trimmed");
  }

  @Test
  public void testTrimSlashesMultipleLeadingAndTrailingSlashes() {
    String input = "///data/test/path///";
    Assertions.assertEquals(
        "data/test/path",
        ADLSLocationUtils.trimSlashes(input),
        "Multiple leading and trailing slashes should be trimmed");
  }

  @Test
  public void testTrimSlashesOnlySlashes() {
    String input = "////";
    Assertions.assertEquals(
        "", ADLSLocationUtils.trimSlashes(input), "Only slashes should result in an empty string");
  }
}
