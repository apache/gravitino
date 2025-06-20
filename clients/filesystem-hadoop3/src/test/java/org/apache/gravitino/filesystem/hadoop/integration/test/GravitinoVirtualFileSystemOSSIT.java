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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.oss.fs.OSSFileSystemProvider;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIf(value = "ossIsConfigured", disabledReason = "OSS is not prepared")
public class GravitinoVirtualFileSystemOSSIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemOSSIT.class);

  public static final String BUCKET_NAME = System.getenv("OSS_BUCKET_NAME");
  public static final String OSS_ACCESS_KEY = System.getenv("OSS_ACCESS_KEY_ID");
  public static final String OSS_SECRET_KEY = System.getenv("OSS_SECRET_ACCESS_KEY");
  public static final String OSS_ENDPOINT = System.getenv("OSS_ENDPOINT");

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aliyun-bundle");
    // Need to download jars to gravitino server
    super.startIntegrationTest();

    // This value can be by tune by the user, please change it accordingly.
    defaultBlockSize = 64 * 1024 * 1024;

    // The default replication factor is 1.
    defaultReplication = 1;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "oss");
    properties.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY);
    properties.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, OSS_SECRET_KEY);
    properties.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, OSS_ENDPOINT);

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
    conf.set(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY);
    conf.set(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, OSS_SECRET_KEY);
    conf.set(OSSProperties.GRAVITINO_OSS_ENDPOINT, OSS_ENDPOINT);
    conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
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
    Configuration ossConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(
            map, OSSFileSystemProvider.GRAVITINO_KEY_TO_OSS_HADOOP_KEY);

    hadoopConfMap.forEach(ossConf::set);

    return ossConf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format("oss://%s/%s", BUCKET_NAME, fileset);
  }

  @Disabled(
      "OSS does not support append, java.io.IOException: The append operation is not supported")
  public void testAppend() throws IOException {}

  protected static boolean ossIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("OSS_ACCESS_KEY_ID"))
        && StringUtils.isNotBlank(System.getenv("OSS_SECRET_ACCESS_KEY"))
        && StringUtils.isNotBlank(System.getenv("OSS_ENDPOINT"))
        && StringUtils.isNotBlank(System.getenv("OSS_BUCKET_NAME"));
  }
}
