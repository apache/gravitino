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
import org.apache.gravitino.cos.fs.COSFileSystemProvider;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.COSProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GVFS integration test against a real Tencent Cloud COS bucket. It is disabled unless the required
 * COS_* environment variables are present, mirroring {@link GravitinoVirtualFileSystemOSSIT}.
 */
@EnabledIf(value = "cosIsConfigured", disabledReason = "COS is not prepared")
public class GravitinoVirtualFileSystemCOSIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemCOSIT.class);

  public static final String BUCKET_NAME = System.getenv("COS_BUCKET_NAME");
  public static final String COS_ACCESS_KEY = System.getenv("COS_ACCESS_KEY_ID");
  public static final String COS_SECRET_KEY = System.getenv("COS_SECRET_ACCESS_KEY");
  public static final String COS_REGION = System.getenv("COS_REGION");
  // Optional: caller may override the endpoint suffix (e.g. for cos-internal endpoints).
  public static final String COS_ENDPOINT = System.getenv("COS_ENDPOINT");

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("tencent-bundle");
    // Need to download jars to gravitino server
    super.startIntegrationTest();

    // Default of fs.cosn.block.size is 128MB.
    defaultBlockSize = 128 * 1024 * 1024;

    // The default replication factor is 1.
    defaultReplication = 1;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "cos");
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    properties.put(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      properties.put(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }

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
    conf.set(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    conf.set(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    conf.set(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      conf.set(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }
    conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
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
   * Remove the `gravitino.bypass` prefix from the configuration and pass it to the real file
   * system. This mirrors {@link
   * GravitinoVirtualFileSystemOSSIT#convertGvfsConfigToRealFileSystemConfig}.
   */
  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration cosConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(
            map, COSFileSystemProvider.GRAVITINO_KEY_TO_COS_HADOOP_KEY);

    hadoopConfMap.forEach(cosConf::set);

    return cosConf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format("cosn://%s/%s", BUCKET_NAME, fileset);
  }

  @Disabled(
      "COS does not support append, java.io.IOException: The append operation is not supported")
  public void testAppend() throws IOException {}

  protected static boolean cosIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("COS_ACCESS_KEY_ID"))
        && StringUtils.isNotBlank(System.getenv("COS_SECRET_ACCESS_KEY"))
        && StringUtils.isNotBlank(System.getenv("COS_REGION"))
        && StringUtils.isNotBlank(System.getenv("COS_BUCKET_NAME"));
  }
}
