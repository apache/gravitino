/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.s3.fs.S3FileSystemProvider;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIf(value = "s3IsConfigured", disabledReason = "s3 with credential is not prepared")
public class GravitinoVirtualFileSystemS3CredentialIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(GravitinoVirtualFileSystemS3CredentialIT.class);

  public static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL");
  public static final String S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL");
  public static final String S3_SECRET_KEY = System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL");
  public static final String S3_ENDPOINT = System.getenv("S3_ENDPOINT_FOR_CREDENTIAL");
  public static final String S3_REGION = System.getenv("S3_REGION_FOR_CREDENTIAL");
  public static final String S3_ROLE_ARN = System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL");

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");

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
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    properties.put(
        "gravitino.bypass.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    properties.put(FILESYSTEM_PROVIDERS, "s3");

    properties.put(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);

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
    conf.set(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    conf.set(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    conf.set(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    conf.set(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    conf.set(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);
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
    return String.format("s3a://%s/%s", BUCKET_NAME, fileset);
  }

  @Disabled(
      "GCS does not support append, java.io.IOException: The append operation is not supported")
  public void testAppend() throws IOException {}

  protected static boolean s3IsConfigured() {
    return StringUtils.isNotBlank(System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ENDPOINT_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_REGION_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL"));
  }
}
