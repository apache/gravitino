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
package org.apache.gravitino.catalog.fileset.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;
import static org.apache.gravitino.storage.GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

@EnabledIf(value = "isGCPConfigured", disabledReason = "GCP is not configured.")
public class FilesetGCSCatalogIT extends FilesetCatalogIT {

  public static final String BUCKET_NAME = System.getenv("GCS_BUCKET_NAME");
  public static final String SERVICE_ACCOUNT_FILE = System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH");

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
    map.put(GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, SERVICE_ACCOUNT_FILE);
    map.put(FILESYSTEM_PROVIDERS, "gcs");
    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }

  @Test
  public void testCreateSchemaAndFilesetWithSpecialLocation() {
    String localCatalogName = GravitinoITUtils.genRandomName("local_catalog");

    String ossLocation = String.format("gs://%s", BUCKET_NAME);
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("location", ossLocation);
    catalogProps.put(GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, SERVICE_ACCOUNT_FILE);
    catalogProps.put(FILESYSTEM_PROVIDERS, "gcs");

    Catalog localCatalog =
        metalake.createCatalog(
            localCatalogName, Catalog.Type.FILESET, provider, "comment", catalogProps);
    Assertions.assertEquals(ossLocation, localCatalog.properties().get("location"));

    // Create schema without specifying location.
    Schema localSchema =
        localCatalog
            .asSchemas()
            .createSchema("local_schema", "comment", ImmutableMap.of("key1", "val1"));

    Fileset localFileset =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema.name(), "local_fileset"),
                "fileset comment",
                Fileset.Type.MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(
        ossLocation + "/local_schema/local_fileset", localFileset.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema.name(), true);

    // Create schema with specifying location.
    Map<String, String> schemaProps = ImmutableMap.of("location", ossLocation);
    Schema localSchema2 =
        localCatalog.asSchemas().createSchema("local_schema2", "comment", schemaProps);
    Assertions.assertEquals(ossLocation, localSchema2.properties().get("location"));

    Fileset localFileset2 =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema2.name(), "local_fileset2"),
                "fileset comment",
                Fileset.Type.MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(ossLocation + "/local_fileset2", localFileset2.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema2.name(), true);

    // Delete catalog
    metalake.dropCatalog(localCatalogName, true);
  }

  private static boolean isGCPConfigured() {
    return StringUtils.isNotBlank(System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH"))
        && StringUtils.isNotBlank(System.getenv("GCS_BUCKET_NAME"));
  }
}
