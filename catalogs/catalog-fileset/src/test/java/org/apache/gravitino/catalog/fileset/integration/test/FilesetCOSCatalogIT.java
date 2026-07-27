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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.COSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;

/**
 * Integration test for the fileset catalog backed by Tencent Cloud COS. It is disabled unless the
 * required environment variables (COS_*) are present, mirroring the OSS / AWS counterparts.
 */
@EnabledIf(value = "cosIsConfigured", disabledReason = "Tencent Cloud COS is not configured.")
public class FilesetCOSCatalogIT extends FilesetCatalogIT {

  public static final String BUCKET_NAME = System.getenv("COS_BUCKET_NAME");
  public static final String COS_ACCESS_KEY = System.getenv("COS_ACCESS_KEY_ID");
  public static final String COS_SECRET_KEY = System.getenv("COS_SECRET_ACCESS_KEY");
  public static final String COS_REGION = System.getenv("COS_REGION");
  // Optional: caller may override the endpoint suffix (e.g. for cos-internal endpoints).
  public static final String COS_ENDPOINT = System.getenv("COS_ENDPOINT");

  @VisibleForTesting
  public void startIntegrationTest() throws Exception {}

  @BeforeAll
  public void setup() throws IOException {
    copyBundleJarsToHadoop("tencent-bundle");

    try {
      super.startIntegrationTest();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start integration test", e);
    }

    metalakeName = GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
    catalogName = GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
    schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    Configuration conf = new Configuration();

    conf.set("fs.cosn.userinfo.secretId", COS_ACCESS_KEY);
    conf.set("fs.cosn.userinfo.secretKey", COS_SECRET_KEY);
    conf.set("fs.cosn.bucket.region", COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      conf.set("fs.cosn.bucket.endpoint_suffix", COS_ENDPOINT);
    }
    conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
    fileSystem = FileSystem.get(URI.create(String.format("cosn://%s", BUCKET_NAME)), conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  @AfterAll
  public void stop() throws IOException {
    super.stop();
  }

  protected String defaultBaseLocation() {
    if (defaultBaseLocation == null) {
      try {
        Path bucket =
            new Path(
                String.format(
                    "cosn://%s/%s",
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
    map.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    map.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    map.put(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      map.put(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }
    map.put(FILESYSTEM_PROVIDERS, "cos");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }

  @Test
  public void testCreateSchemaAndFilesetWithSpecialLocation() {
    String localCatalogName = GravitinoITUtils.genRandomName("local_catalog");

    String cosLocation = String.format("cosn://%s", BUCKET_NAME);
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("location", cosLocation);
    catalogProps.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    catalogProps.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    catalogProps.put(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      catalogProps.put(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }
    catalogProps.put(FILESYSTEM_PROVIDERS, "cos");

    Catalog localCatalog =
        metalake.createCatalog(
            localCatalogName, Catalog.Type.FILESET, provider, "comment", catalogProps);
    Assertions.assertEquals(cosLocation, localCatalog.properties().get("location"));

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
        cosLocation + "/local_schema/local_fileset", localFileset.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema.name(), true);

    // Create schema with specifying location.
    Map<String, String> schemaProps = ImmutableMap.of("location", cosLocation);
    Schema localSchema2 =
        localCatalog.asSchemas().createSchema("local_schema2", "comment", schemaProps);
    Assertions.assertEquals(cosLocation, localSchema2.properties().get("location"));

    Fileset localFileset2 =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema2.name(), "local_fileset2"),
                "fileset comment",
                Fileset.Type.MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(cosLocation + "/local_fileset2", localFileset2.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema2.name(), true);

    // Delete catalog
    metalake.dropCatalog(localCatalogName, true);
  }

  protected static boolean cosIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("COS_ACCESS_KEY_ID"))
        && StringUtils.isNotBlank(System.getenv("COS_SECRET_ACCESS_KEY"))
        && StringUtils.isNotBlank(System.getenv("COS_REGION"))
        && StringUtils.isNotBlank(System.getenv("COS_BUCKET_NAME"));
  }
}
