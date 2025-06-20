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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.s3.fs.S3FileSystemProvider;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class GravitinoVirtualFileSystemS3IT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystemS3IT.class);

  private String bucketName = "s3-bucket-" + UUID.randomUUID().toString().replace("-", "");
  private String accessKey;
  private String secretKey;
  private String s3Endpoint;

  private GravitinoLocalStackContainer gravitinoLocalStackContainer;

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  private void startS3Mocker() {
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
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");

    // Start s3 simulator
    startS3Mocker();

    // Need to download jars to gravitino server
    super.startIntegrationTest();

    // This value can be by tune by the user, please change it accordingly.
    defaultBlockSize = 32 * 1024 * 1024;

    // The value is 1 for S3
    defaultReplication = 1;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("gravitino.bypass.fs.s3a.access.key", accessKey);
    properties.put("gravitino.bypass.fs.s3a.secret.key", secretKey);
    properties.put("gravitino.bypass.fs.s3a.endpoint", s3Endpoint);
    properties.put(
        "gravitino.bypass.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    properties.put(FILESYSTEM_PROVIDERS, "s3");

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
    conf.set(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, accessKey);
    conf.set(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, secretKey);
    conf.set(S3Properties.GRAVITINO_S3_ENDPOINT, s3Endpoint);
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
    Configuration s3Conf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(map, S3FileSystemProvider.GRAVITINO_KEY_TO_S3_HADOOP_KEY);

    hadoopConfMap.forEach(s3Conf::set);

    return s3Conf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format("s3a://%s/%s", bucketName, fileset);
  }

  @Disabled(
      "GCS does not support append, java.io.IOException: The append operation is not supported")
  public void testAppend() throws IOException {}
}
