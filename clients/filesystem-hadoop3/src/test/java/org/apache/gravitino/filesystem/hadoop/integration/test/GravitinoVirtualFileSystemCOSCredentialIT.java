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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.cos.fs.COSFileSystemProvider;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.COSProperties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for the GVFS path against a Tencent Cloud COS catalog whose credentials are
 * vended dynamically via the {@code cos-token} provider (STS AssumeRole). Mirrors {@link
 * GravitinoVirtualFileSystemOSSCredentialIT} so that the two clouds keep parity in coverage.
 *
 * <p>The test is gated on a separate set of {@code *_FOR_CREDENTIAL} env vars (and notably {@code
 * COS_ROLE_ARN_FOR_CREDENTIAL}) so that the static-key COS IT and this STS IT can coexist without
 * accidentally running with stale env config. If {@code COS_EXTERNAL_ID_FOR_CREDENTIAL} is also
 * set, it is propagated to the catalog to exercise the optional {@code cos-external-id} path.
 */
@EnabledIf(value = "cosIsConfigured", disabledReason = "Tencent Cloud COS STS is not prepared")
public class GravitinoVirtualFileSystemCOSCredentialIT extends GravitinoVirtualFileSystemIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(GravitinoVirtualFileSystemCOSCredentialIT.class);

  public static final String BUCKET_NAME = System.getenv("COS_BUCKET_NAME_FOR_CREDENTIAL");
  public static final String COS_ACCESS_KEY = System.getenv("COS_ACCESS_KEY_ID_FOR_CREDENTIAL");
  public static final String COS_SECRET_KEY = System.getenv("COS_SECRET_ACCESS_KEY_FOR_CREDENTIAL");
  public static final String COS_REGION = System.getenv("COS_REGION_FOR_CREDENTIAL");
  public static final String COS_ENDPOINT = System.getenv("COS_ENDPOINT_FOR_CREDENTIAL");
  public static final String COS_ROLE_ARN = System.getenv("COS_ROLE_ARN_FOR_CREDENTIAL");
  // COSCredentialConfig#COS_APP_ID has a NotBlank check and COSTokenGenerator uses it to build
  // the STS resource ARN (e.g. "my-bucket-1259000000"). Missing it would fail catalog creation
  // before any GVFS traffic runs.
  public static final String COS_APP_ID = System.getenv("COS_APP_ID_FOR_CREDENTIAL");
  // Optional external id for AssumeRole; when set the catalog is created with it so that STS
  // rejects calls whose ExternalId does not match the target role's trust policy.
  public static final String COS_EXTERNAL_ID = System.getenv("COS_EXTERNAL_ID_FOR_CREDENTIAL");

  @BeforeAll
  public void startIntegrationTest() {
    // Override parent's @BeforeAll - it's redirected to startUp() below so that we can copy the
    // tencent-bundle JARs to the Gravitino server before booting it.
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("tencent-bundle");
    super.startIntegrationTest();

    // hadoop-cos defaults to 128 MB blocks; must match GravitinoVirtualFileSystemCOSIT so that
    // the parent's testGetDefaultBlockSizes() assertion passes.
    defaultBlockSize = 128 * 1024 * 1024;
    defaultReplication = 1;

    metalakeName = GravitinoITUtils.genRandomName("gvfs_it_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "cos");
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    properties.put(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    properties.put(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      properties.put(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }
    properties.put(COSProperties.GRAVITINO_COS_ROLE_ARN, COS_ROLE_ARN);
    properties.put(COSProperties.GRAVITINO_COS_APP_ID, COS_APP_ID);
    if (StringUtils.isNotBlank(COS_EXTERNAL_ID)) {
      properties.put(COSProperties.GRAVITINO_COS_EXTERNAL_ID, COS_EXTERNAL_ID);
    }
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE);
    // Explicit non-default TTL so downstream tests can assert expireTimeInMs propagation without
    // colliding with the 3600s built-in default.
    properties.put(CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS, "1800");

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
    // Enable credential vending on the GVFS client so that data-plane operations go through the
    // cos-token STS AssumeRole flow (via COSTokenGenerator) instead of falling back to the static
    // AK/SK configured on the catalog. Mirrors the S3/OSS/ABS/GCS credential ITs.
    conf.set("fs.gravitino.enableCredentialVending", "true");

    // Pass the COS endpoint settings to the underlying CosFileSystem so that GVFS can hand off
    // file IO once the STS credentials are vended back to the client.
    conf.set(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, COS_ACCESS_KEY);
    conf.set(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET, COS_SECRET_KEY);
    conf.set(COSProperties.GRAVITINO_COS_REGION, COS_REGION);
    if (StringUtils.isNotBlank(COS_ENDPOINT)) {
      conf.set(COSProperties.GRAVITINO_COS_ENDPOINT, COS_ENDPOINT);
    }
    conf.set("fs.cosn.impl", "org.apache.hadoop.fs.CosFileSystem");
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
   * Strip the {@code gravitino.bypass} prefix and translate Gravitino property keys into their
   * hadoop-cos equivalents. Mirrors the OSS counterpart and the production {@code
   * GravitinoVirtualFileSystem#getConfigMap}.
   */
  protected Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration cosConf = new Configuration();
    Map<String, String> map = Maps.newHashMap();

    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(
            map, COSFileSystemProvider.GRAVITINO_KEY_TO_COS_HADOOP_KEY);

    hadoopConfMap.forEach(cosConf::set);

    return cosConf;
  }

  protected String genStorageLocation(String fileset) {
    return String.format("cosn://%s/%s", BUCKET_NAME, fileset);
  }

  @Disabled(
      "COS does not support HDFS-style append; CosFileSystem throws "
          + "UnsupportedOperationException for append()")
  public void testAppend() throws IOException {}

  /**
   * For the {@code cos-token} STS provider, catalog-scoped credential vending must return an empty
   * array (mirrors {@code oss-token} / {@code s3-token}).
   *
   * <p>Rationale: {@code COSTokenGenerator.generate(...)} bails out with {@code null} whenever the
   * context is not a {@code PathBasedCredentialContext}, and the catalog-level endpoint always
   * hands in a {@code CatalogCredentialContext}. Without this test we would silently regress if
   * someone later flipped that guard - the STS token would leak out at catalog scope with a policy
   * that is not path-restricted.
   */
  @Test
  void testCatalogCredentialsReturnsEmpty() {
    Catalog catalog = metalake.loadCatalog(catalogName);
    Credential[] credentials = catalog.supportsCredentials().getCredentials();
    Assertions.assertEquals(
        0,
        credentials.length,
        "cos-token provider must return no credential at catalog scope (path-less context)");
  }

  /**
   * {@code cos-token-expire-in-secs} configured on the catalog (1800 in {@link #startUp()}) must be
   * honoured end-to-end - i.e. the vended {@code expireTimeInMs} sits ~30 minutes in the future
   * rather than falling back to the 3600s built-in default. Also asserts the three cos-token info
   * fields are present so downstream consumers can rely on the payload shape.
   */
  @Test
  void testFilesetCredentialRespectsExpireInSecs() {
    String filesetName = GravitinoITUtils.genRandomName("cos_cred_ttl");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    Catalog catalog = metalake.loadCatalog(catalogName);

    Fileset fileset =
        catalog
            .asFilesetCatalog()
            .createFileset(
                filesetIdent,
                "fileset for ttl check",
                Fileset.Type.MANAGED,
                genStorageLocation(filesetName),
                ImmutableMap.of());

    try {
      long now = System.currentTimeMillis();
      Credential[] credentials = fileset.supportsCredentials().getCredentials();

      Assertions.assertEquals(1, credentials.length, "expect exactly one cos-token credential");
      Assertions.assertInstanceOf(COSTokenCredential.class, credentials[0]);

      Map<String, String> info = credentials[0].credentialInfo();
      Assertions.assertTrue(
          StringUtils.isNotBlank(info.get(COSTokenCredential.GRAVITINO_COS_SESSION_ACCESS_KEY_ID)),
          "cos-access-key-id must be present");
      Assertions.assertTrue(
          StringUtils.isNotBlank(
              info.get(COSTokenCredential.GRAVITINO_COS_SESSION_SECRET_ACCESS_KEY)),
          "cos-secret-access-key must be present");
      Assertions.assertTrue(
          StringUtils.isNotBlank(info.get(COSTokenCredential.GRAVITINO_COS_SESSION_TOKEN)),
          "cos-security-token must be present");

      long remainingMs = credentials[0].expireTimeInMs() - now;
      // We configured 1800s. Allow a generous +/- 120s window to absorb the round trip to STS and
      // any small clock skew between server and this JVM.
      long lower = 1_680_000L;
      long upper = 1_920_000L;
      Assertions.assertTrue(
          remainingMs >= lower && remainingMs <= upper,
          () ->
              String.format(
                  "expireTimeInMs remaining=%d ms is outside [%d, %d] - cos-token-expire-in-secs=1800 did not propagate",
                  remainingMs, lower, upper));
    } finally {
      catalog.asFilesetCatalog().dropFileset(filesetIdent);
    }
  }

  protected static boolean cosIsConfigured() {
    return StringUtils.isNotBlank(System.getenv("COS_ACCESS_KEY_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("COS_SECRET_ACCESS_KEY_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("COS_BUCKET_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("COS_REGION_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("COS_ROLE_ARN_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("COS_APP_ID_FOR_CREDENTIAL"));
  }
}
