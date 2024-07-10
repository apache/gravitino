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
package com.datastrato.gravitino.catalog.hadoop.integration.test;

import static com.datastrato.gravitino.integration.test.container.S3MockContainer.HTTP_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class HadoopS3IT extends AbstractIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopS3IT.class);
  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("CatalogFilesetIT_s3_metalake");
  private static final String CATALOG_NAME =
      GravitinoITUtils.genRandomName("CatalogFilesetIT_s3_catalog");
  private static final String SCHEMA_NAME =
      GravitinoITUtils.genRandomName("CatalogFilesetIT_s3_schema");
  private static final String FILESET_PROVIDER = "hadoop";
  private static final String DEFAULT_BASE_LOCATION = "s3a://gravitino-fileset-IT/";
  private static final String DEFAULT_AK = "foo";
  private static final String DEFAULT_SK = "bar";

  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static FileSystem s3FileSystem;
  private static String s3Endpoint;

  @BeforeAll
  public static void setup() throws IOException {
    containerSuite.startS3MockContainer();
    s3Endpoint =
        String.format(
            "http://%s:%s/",
            containerSuite.getS3MockContainer().getContainerIpAddress(), HTTP_PORT);

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_BASE_LOCATION);
    conf.set("fs.s3a.access.key", DEFAULT_AK);
    conf.set("fs.s3a.secret.key", DEFAULT_SK);
    conf.set("fs.s3a.endpoint", s3Endpoint);
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    conf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    s3FileSystem = FileSystem.get(conf);

    createMetalake();
    createCatalog();
    createSchema();
  }

  private static void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    assertEquals(0, gravitinoMetalakes.length);

    GravitinoMetalake createdMetalake =
        client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(METALAKE_NAME);
    assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> hadoopS3CatalogProperties =
        ImmutableMap.<String, String>builder()
            .put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .put("fs.s3a.connection.ssl.enabled", "false")
            .put("fs.s3a.path.style.access", "true")
            .put("fs.s3a.access.key", DEFAULT_AK)
            .put("fs.s3a.secret.key", DEFAULT_SK)
            .put("fs.s3a.endpoint", s3Endpoint)
            .build();

    metalake.createCatalog(
        CATALOG_NAME, Catalog.Type.FILESET, FILESET_PROVIDER, "comment", hadoopS3CatalogProperties);

    catalog = metalake.loadCatalog(CATALOG_NAME);
  }

  private static void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put("location", DEFAULT_BASE_LOCATION);
    String comment = "comment";

    catalog.asSchemas().createSchema(SCHEMA_NAME, comment, properties);
    Schema loadSchema = catalog.asSchemas().loadSchema(SCHEMA_NAME);

    assertEquals(SCHEMA_NAME, loadSchema.name());
    assertEquals(comment, loadSchema.comment());
    assertEquals("val1", loadSchema.properties().get("key1"));
    assertEquals("val2", loadSchema.properties().get("key2"));
    assertEquals(DEFAULT_BASE_LOCATION, loadSchema.properties().get("location"));
  }

  @Test
  public void testCreateFilesetOnS3() throws IOException {
    // create fileset
    String filesetName = "test_create_fileset_s3";
    String storageLocation = DEFAULT_BASE_LOCATION + filesetName;
    assertFalse(
        s3FileSystem.exists(new Path(storageLocation)),
        "storage location should not exist before creating");
    Fileset fileset =
        createFileset(
            filesetName,
            "comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of("k1", "v1"));

    // verify fileset is created
    assertFilesetExists(filesetName, storageLocation);
    assertNotNull(fileset, "fileset should be created");
    assertEquals("comment", fileset.comment());
    assertEquals(Fileset.Type.MANAGED, fileset.type());
    assertEquals(storageLocation, fileset.storageLocation());
    assertEquals(1, fileset.properties().size());
    assertEquals("v1", fileset.properties().get("k1"));
  }

  private Fileset createFileset(
      String filesetName,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties) {
    if (storageLocation != null) {
      Path location = new Path(storageLocation);
      try {
        s3FileSystem.deleteOnExit(location);
      } catch (IOException e) {
        LOGGER.warn("Failed to delete location: {}", location, e);
      }
    }
    return catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(SCHEMA_NAME, filesetName),
            comment,
            type,
            storageLocation,
            properties);
  }

  private void assertFilesetExists(String filesetName, String storageLocation) throws IOException {
    assertTrue(
        catalog.asFilesetCatalog().filesetExists(NameIdentifier.of(SCHEMA_NAME, filesetName)),
        "fileset should be exists");
    assertTrue(s3FileSystem.exists(new Path(storageLocation)), "storage location should be exists");
  }

  @AfterAll
  public static void stop() throws IOException {
    Catalog catalog = metalake.loadCatalog(CATALOG_NAME);
    catalog.asSchemas().dropSchema(SCHEMA_NAME, true);
    metalake.dropCatalog(CATALOG_NAME);
    client.dropMetalake(METALAKE_NAME);

    if (s3FileSystem != null) {
      s3FileSystem.close();
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close CloseableGroup", e);
    }
  }
}
