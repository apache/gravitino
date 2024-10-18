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
package org.apache.gravitino.catalog.hadoop.integration.test;

import static org.apache.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled(
    "Disabled due to we don't have a real OSS account to test. If you have a GCP account,"
        + "please change the configuration(BUCKET_NAME, OSS_ACCESS_KEY, OSS_SECRET_KEY, OSS_ENDPOINT) and enable this test.")
public class HadoopOSSCatalogIT extends HadoopCatalogIT {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopOSSCatalogIT.class);
  public static final String BUCKET_NAME = "YOUR_BUCKET";
  public static final String OSS_ACCESS_KEY = "YOUR_OSS_ACCESS_KEY";
  public static final String OSS_SECRET_KEY = "YOUR_OSS_SECRET_KEY";
  public static final String OSS_ENDPOINT = "YOUR_OSS_ENDPOINT";

  @VisibleForTesting
  public void startIntegrationTest() throws Exception {}

  @BeforeAll
  public void setup() throws IOException {
    copyBundleJarsToHadoop("aliyun-bundle");

    try {
      super.startIntegrationTest();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start integration test", e);
    }

    metalakeName = GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
    catalogName = GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
    schemaName = GravitinoITUtils.genRandomName("CatalogFilesetIT_schema");

    schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    Configuration conf = new Configuration();

    conf.set("fs.oss.accessKeyId", OSS_ACCESS_KEY);
    conf.set("fs.oss.accessKeySecret", OSS_SECRET_KEY);
    conf.set("fs.oss.endpoint", OSS_ENDPOINT);
    conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
    fileSystem = FileSystem.get(URI.create(String.format("oss://%s", BUCKET_NAME)), conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() throws IOException {
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog.asSchemas().dropSchema(schemaName, true);
    metalake.dropCatalog(catalogName);
    client.dropMetalake(metalakeName);

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
  }

  protected String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      try {
        Path bucket =
            new Path(
                String.format(
                    "oss://%s/%s",
                    BUCKET_NAME, GravitinoITUtils.genRandomName("CatalogFilesetIT")));
        if (!fileSystem.exists(bucket)) {
          fileSystem.mkdirs(bucket);
        }

        defaultBaseLocation = bucket.toString();
      } catch (IOException e) {
        throw new RuntimeException("Failed to create default base location", e);
      }
    }

    return defaultBaseLocation;
  }

  protected void createCatalog() {
    Map<String, String> map = Maps.newHashMap();
    map.put("gravitino.bypass.fs.oss.accessKeyId", OSS_ACCESS_KEY);
    map.put("gravitino.bypass.fs.oss.accessKeySecret", OSS_SECRET_KEY);
    map.put("gravitino.bypass.fs.oss.endpoint", OSS_ENDPOINT);
    map.put("gravitino.bypass.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
    map.put(FILESYSTEM_PROVIDERS, "oss");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }
}
