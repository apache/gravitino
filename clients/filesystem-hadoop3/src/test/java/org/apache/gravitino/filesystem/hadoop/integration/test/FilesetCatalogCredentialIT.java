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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class FilesetCatalogCredentialIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetCatalogCredentialIT.class);

  public static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
  public static final String S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY_ID");
  public static final String S3_SECRET_KEY = System.getenv("S3_SECRET_ACCESS_KEY");
  public static final String S3_ROLE_ARN = System.getenv("S3_ROLE_ARN");

  private String metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("catalog");
  private String schemaName = GravitinoITUtils.genRandomName("schema");
  private GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");
    // Need to download jars to gravitino server
    super.startIntegrationTest();

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "s3");
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS,
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE
            + ","
            + S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE);
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, "s3.ap-southeast-2.amazonaws.com");
    properties.put(S3Properties.GRAVITINO_S3_REGION, "ap-southeast-2");
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));
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

  protected String genStorageLocation(String fileset) {
    return String.format("s3a://%s/%s", BUCKET_NAME, fileset);
  }

  @Test
  void testGetCatalogCredential() {
    Catalog catalog = metalake.loadCatalog(catalogName);
    Credential[] credentials = catalog.supportsCredentials().getCredentials();
    Assertions.assertEquals(1, credentials.length);
    Assertions.assertTrue(credentials[0] instanceof S3SecretKeyCredential);
  }

  @Test
  void testGetFilesetCredential() {
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_credential");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    String storageLocation = genStorageLocation(filesetName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    Fileset fileset = catalog.asFilesetCatalog().loadFileset(filesetIdent);
    Credential[] credentials = fileset.supportsCredentials().getCredentials();
    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(S3TokenCredential.class, credentials[0]);
  }
}
