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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
@Disabled(
    "Disabled due to we don't have a real GCP account to test. If you have a GCP account,"
        + "please change the configuration(YOUR_KEY_FILE, YOUR_BUCKET) and enable this test.")
public class HadoopGCSCatalogIT extends HadoopCatalogIT {

  public static final String BUCKET_NAME = "YOUR_BUCKET";
  public static final String SERVICE_ACCOUNT_FILE = "YOUR_KEY_FILE";

  @Override
  public void startIntegrationTest() throws Exception {
    // Just overwrite super, do nothing.
  }

  @BeforeAll
  public void setup() throws IOException {
    copyBundleJarsToHadoop("gcp-bundle");

    try {
      super.startIntegrationTest();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    metalakeName = GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
    catalogName = GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
    schemaName = GravitinoITUtils.genRandomName("CatalogFilesetIT_schema");

    schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    Configuration conf = new Configuration();

    conf.set("fs.gs.auth.service.account.enable", "true");
    conf.set("fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_FILE);
    fileSystem = FileSystem.get(URI.create(String.format("gs://%s", BUCKET_NAME)), conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  protected String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      try {
        Path bucket =
            new Path(
                String.format(
                    "gs://%s/%s", BUCKET_NAME, GravitinoITUtils.genRandomName("CatalogFilesetIT")));
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
    map.put("gravitino.bypass.fs.gs.auth.service.account.enable", "true");
    map.put("gravitino.bypass.fs.gs.auth.service.account.json.keyfile", SERVICE_ACCOUNT_FILE);
    map.put(FILESYSTEM_PROVIDERS, "gcs");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }
}
