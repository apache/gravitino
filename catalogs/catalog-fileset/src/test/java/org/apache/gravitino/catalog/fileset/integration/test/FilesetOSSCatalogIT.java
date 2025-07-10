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
import org.apache.gravitino.storage.OSSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIf(value = "ossIsConfigured", disabledReason = "OSS is not configured.")
public class FilesetOSSCatalogIT extends FilesetCatalogIT {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetOSSCatalogIT.class);
  public static final String BUCKET_NAME = System.getenv("OSS_BUCKET_NAME");
  public static final String OSS_ACCESS_KEY = System.getenv("OSS_ACCESS_KEY_ID");
  public static final String OSS_SECRET_KEY = System.getenv("OSS_SECRET_ACCESS_KEY");
  public static final String OSS_ENDPOINT = System.getenv("OSS_ENDPOINT");

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
    metalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);

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
    map.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY);
    map.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, OSS_SECRET_KEY);
    map.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, OSS_ENDPOINT);
    map.put(FILESYSTEM_PROVIDERS, "oss");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }

  @Test
  public void testCreateSchemaAndFilesetWithSpecialLocation() {
    String localCatalogName = GravitinoITUtils.genRandomName("local_catalog");

    String ossLocation = String.format("oss://%s", BUCKET_NAME);
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("location", ossLocation);
    catalogProps.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, OSS_ENDPOINT);
    catalogProps.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY);
    catalogProps.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, OSS_SECRET_KEY);
    catalogProps.put(FILESYSTEM_PROVIDERS, "oss");

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

  protected static boolean ossIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("OSS_ACCESS_KEY_ID"))
        && StringUtils.isNotBlank(System.getenv("OSS_SECRET_ACCESS_KEY"))
        && StringUtils.isNotBlank(System.getenv("OSS_ENDPOINT"))
        && StringUtils.isNotBlank(System.getenv("OSS_BUCKET_NAME"));
  }
}
