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

package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.gcs.fs.GCSFileSystemProvider;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIf(value = "isGCPConfigured", disabledReason = "GCP is not configured")
public class GravitinoVirtualFileSystemGCSIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemGCSIT.class);

  public static final String BUCKET_NAME = System.getenv("GCS_BUCKET_NAME");
  public static final String SERVICE_ACCOUNT_FILE = System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH");

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    // Copy the GCP jars to the gravitino server if in deploy mode.
    copyBundleJarsToHadoop("gcp-bundle");
    // Need to download jars to gravitino server
    super.startIntegrationTest();

    // This value can be by tune by the user, please change it accordingly.
    defaultBlockSize = 64 * 1024 * 1024;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "gcs");
    properties.put(
        "gravitino.bypass.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_FILE);

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
    conf.set(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, SERVICE_ACCOUNT_FILE);
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
    Configuration gcsConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(
            map, GCSFileSystemProvider.GRAVITINO_KEY_TO_GCS_HADOOP_KEY);

    hadoopConfMap.forEach(gcsConf::set);

    return gcsConf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format("gs://%s/%s", BUCKET_NAME, fileset);
  }

  @Disabled(
      "GCS does not support append, java.io.IOException: The append operation is not supported")
  public void testAppend() throws IOException {}

  private static boolean isGCPConfigured() {
    return StringUtils.isNotBlank(System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH"))
        && StringUtils.isNotBlank(System.getenv("GCS_BUCKET_NAME"));
  }
}
