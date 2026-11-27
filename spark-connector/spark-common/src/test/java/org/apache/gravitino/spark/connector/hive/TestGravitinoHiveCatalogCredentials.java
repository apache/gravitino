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

package org.apache.gravitino.spark.connector.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.utils.LocalSparkSessions;
import org.apache.hadoop.conf.Configuration;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * GravitinoHiveCatalog writes the credentials its Gravitino catalog vends to where the object
 * stores read them: S3 and Azure keys into the Spark catalog options, and OSS credentials, the
 * vended pair or the pair given as options, into the Hadoop configuration of the kyuubi catalog,
 * because catalog option keys reach the catalog in lower case and the OSS names do not survive
 * that. These tests pin three things about it: the keys are the forms the filesystems read, a key
 * the user already configured is not overridden, and the keys reach the Hadoop configuration the
 * kyuubi catalog reads data with.
 */
@Timeout(60)
public class TestGravitinoHiveCatalogCredentials {

  private static final String S3_ACCESS_KEY = "fs.s3a.access.key";
  private static final String S3_SECRET_KEY = "fs.s3a.secret.key";
  private static final String OSS_ACCESS_KEY = "fs.oss.accessKeyId";
  private static final String OSS_SECRET_KEY = "fs.oss.accessKeySecret";
  private static final String AZURE_KEY = "fs.azure.account.key.storageacct.dfs.core.windows.net";
  private static final String AZURE_ACCOUNT_WIDE_KEY = "fs.azure.account.key";
  private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";

  /** The catalog properties the converter requires; the metastore is never contacted here. */
  private static final Map<String, String> CATALOG_PROPERTIES =
      ImmutableMap.of(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI, "thrift://localhost:9083");

  private static final Credential[] ALL_CREDENTIALS = {
    new S3SecretKeyCredential("s3-ak", "s3-sk"),
    new OSSSecretKeyCredential("oss-id", "oss-secret"),
    new AzureAccountKeyCredential("storageacct", "azure-key")
  };

  @BeforeAll
  static void initCatalogManager() {
    // GravitinoHiveCatalog extends BaseCatalog, whose constructor requires the manager.
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
  }

  @AfterAll
  static void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  // ---------------------------------------------------------------------------------------
  // The options applyOptionCredentials writes
  // ---------------------------------------------------------------------------------------

  @Test
  void testVendedCredentialsUseFilesystemKeyNames() {
    Map<String, String> props = new HashMap<>();

    GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

    assertEquals(
        ImmutableMap.of(
            S3_ACCESS_KEY, "s3-ak",
            S3_SECRET_KEY, "s3-sk",
            AZURE_KEY, "azure-key"),
        props,
        "S3 and Azure keys are written; OSS credentials travel elsewhere");
  }

  @Test
  void testConfiguredKeysAreNotOverridden() {
    Map<String, String> configured =
        ImmutableMap.of(
            S3_ACCESS_KEY, "user-ak",
            S3_SECRET_KEY, "user-sk",
            AZURE_KEY, "user-azure-key");

    Map<String, String> props = new HashMap<>(configured);

    GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

    assertEquals(configured, props, "a configured credential must keep winning");
  }

  @Test
  void testHalfConfiguredPairIsLeftAlone() {
    for (String configured : new String[] {S3_ACCESS_KEY, S3_SECRET_KEY}) {
      Map<String, String> props = new HashMap<>(ImmutableMap.of(configured, "user"));

      GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

      assertEquals(
          ImmutableMap.of(configured, "user", AZURE_KEY, "azure-key"),
          props,
          "one vended half beside one configured half cannot authenticate");
    }
  }

  @Test
  void testConfiguredKeyIsRecognizedRegardlessOfCase() {
    Map<String, String> props =
        new HashMap<>(
            ImmutableMap.of("FS.S3A.ACCESS.KEY", "user-ak", "FS.S3A.SECRET.KEY", "user-sk"));

    GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

    // Catalog options reach the catalog in lower case, so this entry reaches S3A as
    // fs.s3a.access.key all the same. The vended pair is not written beside it.
    assertEquals(
        ImmutableMap.of(
            "FS.S3A.ACCESS.KEY", "user-ak",
            "FS.S3A.SECRET.KEY", "user-sk",
            AZURE_KEY, "azure-key"),
        props,
        "the user's entries stay as they are and no vended S3 key is added beside them");
  }

  @Test
  void testAzureKeyIsBuiltInLowerCaseForAnUpperCaseAccountName() {
    Credential[] vended = {new AzureAccountKeyCredential("StorageAcct", "azure-key")};
    Map<String, String> props = new HashMap<>();

    GravitinoHiveCatalog.applyOptionCredentials(vended, props, null);

    assertEquals(ImmutableMap.of(AZURE_KEY, "azure-key"), props);
  }

  @Test
  void testSessionAzureKeyIsFoundForAnUpperCaseAccountName() {
    // The option key reaches the catalog in lower case, so the session key that the vended one
    // would be applied over is the lower-case one.
    Credential[] vended = {new AzureAccountKeyCredential("StorageAcct", "azure-key")};
    Configuration session = new Configuration(false);
    session.set(AZURE_KEY, "user-azure-key");
    Map<String, String> props = new HashMap<>();

    GravitinoHiveCatalog.applyOptionCredentials(vended, props, session);

    assertTrue(props.isEmpty(), "the session's account key must keep winning");
  }

  @Test
  void testAzureKeyIsWrittenForAnAccountWithAnotherAuthType() {
    // hadoop-azure reads the account key only when the account authenticates with a shared key,
    // so the key written beside an SAS setup is unused. Whether a setting is used at all is the
    // filesystem's decision; this connector only keeps configured keys from being replaced.
    String authType = "fs.azure.account.auth.type.storageacct.dfs.core.windows.net";
    Map<String, String> props = new HashMap<>(ImmutableMap.of(authType, "SAS"));

    GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

    assertEquals("SAS", props.get(authType), "the user's setting stays as it is");
    assertEquals(
        "azure-key", props.get(AZURE_KEY), "the account key is written as for any account");
  }

  @Test
  void testAccountWideAzureKeyIsNotOverridden() {
    // hadoop-azure falls back to this one key for an account that has no key of its own, so a
    // vended key written under the account's own name would silently take its place.
    Map<String, String> props =
        new HashMap<>(ImmutableMap.of(AZURE_ACCOUNT_WIDE_KEY, "user-azure-key"));

    GravitinoHiveCatalog.applyOptionCredentials(ALL_CREDENTIALS, props, null);

    assertEquals("user-azure-key", props.get(AZURE_ACCOUNT_WIDE_KEY));
    assertNull(props.get(AZURE_KEY), "the account's own key must not replace the account-wide one");
  }

  @Test
  void testUnrecognizedCredentialIsSkipped() {
    Credential unknown = mock(Credential.class);
    Map<String, String> props = new HashMap<>();

    GravitinoHiveCatalog.applyOptionCredentials(new Credential[] {unknown}, props, null);

    assertTrue(props.isEmpty(), "an unrecognized credential writes nothing");
  }

  // ---------------------------------------------------------------------------------------
  // OSS credentials, written to the kyuubi catalog's own configuration
  // ---------------------------------------------------------------------------------------

  @Test
  void testVendedOssPairIsWrittenUnderTheNamesTheOssFilesystemReads() {
    Configuration kyuubi = new Configuration(false);

    GravitinoHiveCatalog.applyOSSCredential(ALL_CREDENTIALS, kyuubi, new HashMap<>(), null);

    assertEquals(
        "oss-id", kyuubi.get(OSS_ACCESS_KEY), "exactly this name, upper-case letters kept");
    assertEquals("oss-secret", kyuubi.get(OSS_SECRET_KEY));
  }

  @Test
  void testOssPairGivenAsOptionsIsWrittenUnderTheNamesTheOssFilesystemReads() {
    // Spark hands catalog options on in lower case and a bypass property keeps its case: both
    // forms are found and written under the names the OSS filesystem reads.
    for (Map<String, String> given :
        ImmutableList.of(
            ImmutableMap.of(
                "fs.oss.accesskeyid", "user-oss-id",
                "fs.oss.accesskeysecret", "user-oss-secret"),
            ImmutableMap.of(OSS_ACCESS_KEY, "user-oss-id", OSS_SECRET_KEY, "user-oss-secret"))) {
      Configuration kyuubi = new Configuration(false);

      GravitinoHiveCatalog.applyOSSCredential(ALL_CREDENTIALS, kyuubi, new HashMap<>(given), null);

      assertEquals(
          "user-oss-id", kyuubi.get(OSS_ACCESS_KEY), "the user's pair, not the vended one");
      assertEquals(
          "user-oss-secret", kyuubi.get(OSS_SECRET_KEY), "the user's pair, not the vended one");
    }
  }

  @Test
  void testHalfConfiguredOssPairGivenAsOptionsIsNotCompleted() {
    for (String half : new String[] {OSS_ACCESS_KEY, OSS_SECRET_KEY}) {
      Configuration kyuubi = new Configuration(false);

      GravitinoHiveCatalog.applyOSSCredential(
          ALL_CREDENTIALS, kyuubi, new HashMap<>(ImmutableMap.of(half, "user")), null);

      assertEquals("user", kyuubi.get(half), "the half the user gave is written");
      String other = half.equals(OSS_ACCESS_KEY) ? OSS_SECRET_KEY : OSS_ACCESS_KEY;
      assertNull(kyuubi.get(other), "the vended half is not added to it");
    }
  }

  @Test
  void testVendedOssPairIsNotWrittenWhenEitherKeyIsConfiguredInTheSession() {
    for (String configured : new String[] {OSS_ACCESS_KEY, OSS_SECRET_KEY}) {
      Configuration session = new Configuration(false);
      session.set(configured, "session-value");
      Configuration kyuubi = new Configuration(false);

      GravitinoHiveCatalog.applyOSSCredential(ALL_CREDENTIALS, kyuubi, new HashMap<>(), session);

      assertNull(kyuubi.get(OSS_ACCESS_KEY), "a key configured at session level keeps winning");
      assertNull(kyuubi.get(OSS_SECRET_KEY), "a key configured at session level keeps winning");
    }
  }

  @Test
  void testOssHalfGivenAsOptionsIsNotCompletedFromTheVendedPair() {
    // The option half arrives lower-cased, so it is written here; the session keeps its own half,
    // and no vended half is added.
    Configuration session = new Configuration(false);
    session.set(OSS_SECRET_KEY, "session-secret");
    Configuration kyuubi = new Configuration(false);

    GravitinoHiveCatalog.applyOSSCredential(
        ALL_CREDENTIALS,
        kyuubi,
        new HashMap<>(ImmutableMap.of(OSS_ACCESS_KEY, "user-oss-id")),
        session);

    assertEquals("user-oss-id", kyuubi.get(OSS_ACCESS_KEY), "the half the user gave is written");
    assertNull(kyuubi.get(OSS_SECRET_KEY), "no vended secret is added to the user's half");
  }

  @Test
  void testOssKeyGivenAsOptionsWinsOverTheSessionKey() {
    // The catalog applies every other option over the session's configuration; OSS options,
    // written here instead, rank the same way.
    for (String key : new String[] {OSS_ACCESS_KEY, OSS_SECRET_KEY}) {
      Configuration session = new Configuration(false);
      session.set(key, "session-value");
      Configuration kyuubi = new Configuration(false);

      GravitinoHiveCatalog.applyOSSCredential(
          ALL_CREDENTIALS, kyuubi, new HashMap<>(ImmutableMap.of(key, "user")), session);

      assertEquals("user", kyuubi.get(key), "the option is written over the session's key");
      String other = key.equals(OSS_ACCESS_KEY) ? OSS_SECRET_KEY : OSS_ACCESS_KEY;
      assertNull(kyuubi.get(other), "the vended half is not added");
    }
  }

  @Test
  void testBlankOssOptionDoesNotKeepTheVendedPairFromBeingWritten() {
    Configuration kyuubi = new Configuration(false);

    GravitinoHiveCatalog.applyOSSCredential(
        ALL_CREDENTIALS,
        kyuubi,
        new HashMap<>(ImmutableMap.of(OSS_ACCESS_KEY, " ", OSS_SECRET_KEY, "")),
        null);

    assertEquals("oss-id", kyuubi.get(OSS_ACCESS_KEY), "a blank value does not configure the key");
    assertEquals("oss-secret", kyuubi.get(OSS_SECRET_KEY));
  }

  // ---------------------------------------------------------------------------------------
  // What reaches the kyuubi catalog, through the real create-and-initialize path
  // ---------------------------------------------------------------------------------------

  @Test
  void testVendedKeysReachTheKyuubiConfiguration(@TempDir Path warehouse) {
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, ImmutableMap.of())) {
      dropS3KeysFromTheEnvironment(local);
      Configuration vended =
          kyuubiConfiguration(catalogVending(ALL_CREDENTIALS), ImmutableMap.of());

      assertEquals("s3-ak", vended.get(S3_ACCESS_KEY), "through the options");
      assertEquals("s3-sk", vended.get(S3_SECRET_KEY), "through the options");
      assertEquals("oss-id", vended.get(OSS_ACCESS_KEY), "through the catalog's configuration");
      assertEquals("oss-secret", vended.get(OSS_SECRET_KEY), "through the catalog's configuration");
      assertEquals("azure-key", vended.get(AZURE_KEY), "through the options");
      assertNull(vended.get("hadoop.fs.s3a.access.key"), "the old key is no longer written");
    }
  }

  @Test
  void testOssPairGivenAsCatalogOptionsReachesTheKyuubiConfiguration(@TempDir Path warehouse) {
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, ImmutableMap.of())) {
      // The options arrive at the catalog lower-cased, which the OSS filesystem does not read.
      Configuration fromOptions =
          kyuubiConfiguration(
              catalogVending(ALL_CREDENTIALS),
              ImmutableMap.of(OSS_ACCESS_KEY, "user-oss-id", OSS_SECRET_KEY, "user-oss-secret"));

      assertEquals("user-oss-id", fromOptions.get(OSS_ACCESS_KEY), "the user's pair is written");
      assertEquals(
          "user-oss-secret", fromOptions.get(OSS_SECRET_KEY), "the user's pair is written");
    }
  }

  @Test
  void testNothingIsWrittenWhenTheCatalogDoesNotSupportVending(@TempDir Path warehouse) {
    Catalog client = mock(Catalog.class);
    when(client.supportsCredentials()).thenThrow(new UnsupportedOperationException("no vending"));
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, ImmutableMap.of())) {
      dropS3KeysFromTheEnvironment(local);
      assertNoVendedKey(kyuubiConfiguration(client, ImmutableMap.of()));
    }
  }

  @Test
  void testNothingIsWrittenWhenTheCatalogVendsNothing(@TempDir Path warehouse) {
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, ImmutableMap.of())) {
      dropS3KeysFromTheEnvironment(local);
      assertNoVendedKey(kyuubiConfiguration(catalogVending(), ImmutableMap.of()));
    }
  }

  @Test
  void testStartupAndSessionKeysWinOverVendedOnes(@TempDir Path warehouse) {
    Map<String, String> startup =
        ImmutableMap.of(
            SPARK_HADOOP_PREFIX + S3_ACCESS_KEY, "startup-ak",
            SPARK_HADOOP_PREFIX + S3_SECRET_KEY, "startup-sk");
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, startup)) {
      SparkSession spark = local.spark();
      // Given at startup through spark.hadoop.*, hence in the SparkContext's configuration.
      Configuration fromStartup =
          kyuubiConfiguration(catalogVending(ALL_CREDENTIALS), ImmutableMap.of());
      assertEquals("startup-ak", fromStartup.get(S3_ACCESS_KEY), "spark.hadoop.* at startup");
      assertEquals("startup-sk", fromStartup.get(S3_SECRET_KEY), "spark.hadoop.* at startup");

      // Set on the session itself before the catalog is initialized.
      spark.conf().set(OSS_ACCESS_KEY, "runtime-oss-id");
      Configuration fromSession =
          kyuubiConfiguration(catalogVending(ALL_CREDENTIALS), ImmutableMap.of());
      assertEquals("runtime-oss-id", fromSession.get(OSS_ACCESS_KEY), "set on the session");
      assertNull(fromSession.get(OSS_SECRET_KEY), "the vended secret is not added to it");

      // The Azure key configured for all accounts, which hadoop-azure falls back to.
      spark.conf().set(AZURE_ACCOUNT_WIDE_KEY, "runtime-azure-key");
      Configuration accountWide =
          kyuubiConfiguration(catalogVending(ALL_CREDENTIALS), ImmutableMap.of());
      assertEquals(
          "runtime-azure-key", accountWide.get(AZURE_ACCOUNT_WIDE_KEY), "set on the session");
      assertNull(accountWide.get(AZURE_KEY), "the vended account key must not take its place");
    }
  }

  @Test
  void testBlankOptionDoesNotHideASessionKey(@TempDir Path warehouse) {
    try (LocalSparkSessions.Session local = newLocalSession(warehouse, ImmutableMap.of())) {
      SparkSession spark = local.spark();
      spark.conf().set(S3_ACCESS_KEY, "runtime-ak");
      spark.conf().set(S3_SECRET_KEY, "runtime-sk");

      // The catalog applies its options last, so the blank entry would be applied over the
      // session's value if it were left in place.
      Configuration conf =
          kyuubiConfiguration(
              catalogVending(ALL_CREDENTIALS),
              ImmutableMap.of(S3_ACCESS_KEY, " ", S3_SECRET_KEY, ""));

      assertEquals("runtime-ak", conf.get(S3_ACCESS_KEY), "the session's key must survive");
      assertEquals("runtime-sk", conf.get(S3_SECRET_KEY), "the session's key must survive");
    }
  }

  private static void assertNoVendedKey(Configuration conf) {
    assertNull(conf.get(S3_ACCESS_KEY), "no S3 key without a vended credential");
    assertNull(conf.get(OSS_ACCESS_KEY), "no OSS key without a vended credential");
    assertNull(conf.get(AZURE_KEY), "no Azure key without a vended credential");
  }

  /**
   * Spark copies {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY} from the environment
   * into the SparkContext's Hadoop configuration as S3A keys, which would count as configured.
   */
  private static void dropS3KeysFromTheEnvironment(LocalSparkSessions.Session local) {
    Configuration contextConf = local.spark().sparkContext().hadoopConfiguration();
    contextConf.unset(S3_ACCESS_KEY);
    contextConf.unset(S3_SECRET_KEY);
  }

  private static Catalog catalogVending(Credential... credentials) {
    Catalog catalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(catalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(credentials);
    return catalog;
  }

  /** The Hadoop configuration the real kyuubi catalog ends up with for the given options. */
  private static Configuration kyuubiConfiguration(Catalog client, Map<String, String> options) {
    return new TestableHiveCatalog(client).initializeKyuubiCatalog(options).hadoopConfiguration();
  }

  private static LocalSparkSessions.Session newLocalSession(
      Path warehouse, Map<String, String> configs) {
    Map<String, String> all = new HashMap<>(configs);
    // The real kyuubi HiveTableCatalog only initializes on a session with Hive support.
    all.put("spark.sql.catalogImplementation", "hive");
    all.put("spark.sql.warehouse.dir", warehouse.toUri().toString());
    return LocalSparkSessions.create("test-hive-catalog-credentials", all);
  }

  /** A Hive catalog over a mocked Gravitino client, exposing the catalog creation path. */
  private static final class TestableHiveCatalog extends GravitinoHiveCatalog {

    private final Catalog client;

    private TestableHiveCatalog(Catalog client) {
      this.client = client;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return client;
    }

    private HiveTableCatalog initializeKyuubiCatalog(Map<String, String> options) {
      return (HiveTableCatalog)
          createAndInitSparkCatalog(
              "hive", new CaseInsensitiveStringMap(options), CATALOG_PROPERTIES);
    }
  }
}
