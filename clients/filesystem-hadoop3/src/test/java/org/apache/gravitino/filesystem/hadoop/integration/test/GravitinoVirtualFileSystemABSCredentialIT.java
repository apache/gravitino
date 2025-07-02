/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.abs.fs.AzureFileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIf("absIsConfigured")
public class GravitinoVirtualFileSystemABSCredentialIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(GravitinoVirtualFileSystemABSCredentialIT.class);

  public static final String ABS_ACCOUNT_NAME = System.getenv("ABS_ACCOUNT_NAME_FOR_CREDENTIAL");
  public static final String ABS_ACCOUNT_KEY = System.getenv("ABS_ACCOUNT_KEY_FOR_CREDENTIAL");
  public static final String ABS_CONTAINER_NAME =
      System.getenv("ABS_CONTAINER_NAME_FOR_CREDENTIAL");
  public static final String ABS_TENANT_ID = System.getenv("ABS_TENANT_ID_FOR_CREDENTIAL");
  public static final String ABS_CLIENT_ID = System.getenv("ABS_CLIENT_ID_FOR_CREDENTIAL");
  public static final String ABS_CLIENT_SECRET = System.getenv("ABS_CLIENT_SECRET_FOR_CREDENTIAL");

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    // Copy the Azure jars to the gravitino server if in deploy mode.
    copyBundleJarsToHadoop("azure-bundle");
    // Need to download jars to gravitino server
    super.startIntegrationTest();

    // This value can be by tune by the user, please change it accordingly.
    defaultBlockSize = 32 * 1024 * 1024;

    // This value is 1 for ABS, 3 for GCS, and 1 for S3A.
    defaultReplication = 1;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();

    properties.put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, ABS_ACCOUNT_NAME);
    properties.put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY, ABS_ACCOUNT_KEY);
    properties.put(AzureProperties.GRAVITINO_AZURE_CLIENT_ID, ABS_CLIENT_ID);
    properties.put(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET, ABS_CLIENT_SECRET);
    properties.put(AzureProperties.GRAVITINO_AZURE_TENANT_ID, ABS_TENANT_ID);
    properties.put(CredentialConstants.CREDENTIAL_PROVIDERS, "adls-token");

    properties.put(FILESYSTEM_PROVIDERS, AzureFileSystemProvider.ABS_PROVIDER_NAME);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

    conf.set("fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    conf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    conf.set("fs.gvfs.impl.disable.cache", "true");
    conf.set("fs.gravitino.server.uri", serverUri);
    conf.set("fs.gravitino.client.metalake", metalakeName);

    // Pass this configuration to the real file system
    conf.set(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, ABS_ACCOUNT_NAME);
    conf.set(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY, ABS_ACCOUNT_KEY);
    conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");

    conf.set("fs.gravitino.client.useCloudStoreCredential", "true");
  }

  @AfterAll
  public void tearDown() throws IOException {
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  /**
   * Remove the `gravitino.bypass` prefix from the configuration and pass it to the real file system
   * This method corresponds to the method org.apache.gravitino.filesystem.hadoop
   * .GravitinoVirtualFileSystem#getConfigMap(Configuration) in the original code.
   */
  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration absConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap = FileSystemUtils.toHadoopConfigMap(map, ImmutableMap.of());

    if (gvfsConf.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME) != null
        && gvfsConf.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY) != null) {
      hadoopConfMap.put(
          String.format(
              "fs.azure.account.key.%s.dfs.core.windows.net",
              gvfsConf.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME)),
          gvfsConf.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
    }

    hadoopConfMap.forEach(absConf::set);

    return absConf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format(
        "%s://%s@%s.dfs.core.windows.net/%s",
        AzureFileSystemProvider.ABS_PROVIDER_SCHEME, ABS_CONTAINER_NAME, ABS_ACCOUNT_NAME, fileset);
  }

  @Disabled("java.lang.UnsupportedOperationException: Append Support not enabled")
  public void testAppend() throws IOException {}

  private static boolean absIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("ABS_ACCOUNT_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("ABS_ACCOUNT_KEY_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("ABS_CONTAINER_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("ABS_TENANT_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("ABS_CLIENT_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("ABS_CLIENT_SECRET_FOR_CREDENTIAL"));
  }
}
