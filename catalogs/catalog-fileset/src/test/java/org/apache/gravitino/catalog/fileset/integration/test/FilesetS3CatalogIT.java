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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-test")
public class FilesetS3CatalogIT extends FilesetCatalogIT {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetS3CatalogIT.class);
  private String bucketName = "s3-bucket-" + UUID.randomUUID().toString().replace("-", "");
  private String accessKey;
  private String secretKey;
  private String s3Endpoint;

  private GravitinoLocalStackContainer gravitinoLocalStackContainer;

  @VisibleForTesting
  public void startIntegrationTest() throws Exception {}

  @Override
  protected void startNecessaryContainer() {

    containerSuite.startLocalStackContainer();
    gravitinoLocalStackContainer = containerSuite.getLocalStackContainer();

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result =
                    gravitinoLocalStackContainer.executeInContainer(
                        "awslocal", "iam", "create-user", "--user-name", "anonymous");
                return result.getExitCode() == 0;
              } catch (Exception e) {
                LOG.info("LocalStack is not ready yet for: ", e);
                return false;
              }
            });

    gravitinoLocalStackContainer.executeInContainer("awslocal", "s3", "mb", "s3://" + bucketName);

    Container.ExecResult result =
        gravitinoLocalStackContainer.executeInContainer(
            "awslocal", "iam", "create-access-key", "--user-name", "anonymous");

    gravitinoLocalStackContainer.executeInContainer(
        "awslocal",
        "s3api",
        "put-bucket-acl",
        "--bucket",
        "my-test-bucket",
        "--acl",
        "public-read-write");

    // Get access key and secret key from result
    String[] lines = result.getStdout().split("\n");
    accessKey = lines[3].split(":")[1].trim().substring(1, 21);
    secretKey = lines[5].split(":")[1].trim().substring(1, 41);

    LOG.info("Access key: " + accessKey);
    LOG.info("Secret key: " + secretKey);

    s3Endpoint =
        String.format("http://%s:%d", gravitinoLocalStackContainer.getContainerIpAddress(), 4566);
  }

  @BeforeAll
  public void setup() throws IOException {
    copyBundleJarsToHadoop("aws-bundle");

    try {
      super.startIntegrationTest();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start integration test", e);
    }

    startNecessaryContainer();

    metalakeName = GravitinoITUtils.genRandomName("CatalogFilesetIT_metalake");
    catalogName = GravitinoITUtils.genRandomName("CatalogFilesetIT_catalog");
    schemaName = GravitinoITUtils.genRandomName("CatalogFilesetIT_schema");

    schemaName = GravitinoITUtils.genRandomName(SCHEMA_PREFIX);
    Configuration conf = new Configuration();

    conf.set("fs.s3a.access.key", accessKey);
    conf.set("fs.s3a.secret.key", secretKey);
    conf.set("fs.s3a.endpoint", s3Endpoint);
    conf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    fileSystem = FileSystem.get(URI.create(String.format("s3a://%s", bucketName)), conf);

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
                    "s3a://%s/%s", bucketName, GravitinoITUtils.genRandomName("CatalogFilesetIT")));
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
    map.put(S3Properties.GRAVITINO_S3_ENDPOINT, s3Endpoint);
    map.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    map.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretKey);
    map.put(FILESYSTEM_PROVIDERS, "s3");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }

  @Test
  public void testCreateSchemaAndFilesetWithSpecialLocation() {
    String localCatalogName = GravitinoITUtils.genRandomName("local_catalog");

    String s3Location = String.format("s3a://%s", bucketName);
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("location", s3Location);
    catalogProps.put(S3Properties.GRAVITINO_S3_ENDPOINT, s3Endpoint);
    catalogProps.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    catalogProps.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretKey);
    catalogProps.put(FILESYSTEM_PROVIDERS, "s3");

    Catalog localCatalog =
        metalake.createCatalog(
            localCatalogName, Catalog.Type.FILESET, provider, "comment", catalogProps);
    Assertions.assertEquals(s3Location, localCatalog.properties().get("location"));

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
        s3Location + "/local_schema/local_fileset", localFileset.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema.name(), true);

    // Create schema with specifying location.
    Map<String, String> schemaProps = ImmutableMap.of("location", s3Location);
    Schema localSchema2 =
        localCatalog.asSchemas().createSchema("local_schema2", "comment", schemaProps);
    Assertions.assertEquals(s3Location, localSchema2.properties().get("location"));

    Fileset localFileset2 =
        localCatalog
            .asFilesetCatalog()
            .createFileset(
                NameIdentifier.of(localSchema2.name(), "local_fileset2"),
                "fileset comment",
                Fileset.Type.MANAGED,
                null,
                ImmutableMap.of("k1", "v1"));
    Assertions.assertEquals(s3Location + "/local_fileset2", localFileset2.storageLocation());

    // Delete schema
    localCatalog.asSchemas().dropSchema(localSchema2.name(), true);

    // Delete catalog
    metalake.dropCatalog(localCatalogName, true);
  }
}
