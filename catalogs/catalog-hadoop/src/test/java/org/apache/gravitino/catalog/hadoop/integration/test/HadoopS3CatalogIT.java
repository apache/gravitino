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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-test")
public class HadoopS3CatalogIT extends HadoopCatalogIT {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopOSSCatalogIT.class);
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
    map.put("gravitino.bypass.fs.s3a.access.key", accessKey);
    map.put("gravitino.bypass.fs.s3a.secret.key", secretKey);
    map.put("gravitino.bypass.fs.s3a.endpoint", s3Endpoint);
    map.put(
        "gravitino.bypass.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    map.put(FILESYSTEM_PROVIDERS, "s3");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, provider, "comment", map);

    catalog = metalake.loadCatalog(catalogName);
  }

  protected String generateLocation(String filesetName) {
    return String.format("%s/%s", defaultBaseLocation, filesetName);
  }
}
