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

package org.apache.gravitino.spark.connector.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin.DynamicBearerTokenProvider;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests what the plugin does with the bindings it is given. The bindings here are made up, so these
 * assertions hold on every Spark version and do not restate what the version modules declare.
 */
public class TestGravitinoDriverPlugin {

  private static final String COMMA = ",";
  private static final String AUTHZ_EXTENSION = "org.example.AuthorizationExtensions";
  private static final String USER_EXTENSION = "org.example.UserExtensions";
  private static final String PAIMON_CATALOG = "org.example.PaimonCatalog";

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testAlwaysRegistersTheBoundAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);

    new GravitinoDriverPlugin(withoutPaimon()).registerSqlExtensions(sparkConf);

    assertEquals(AUTHZ_EXTENSION, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testDoesNotDuplicateAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), AUTHZ_EXTENSION);

    new GravitinoDriverPlugin(withoutPaimon()).registerSqlExtensions(sparkConf);

    assertEquals(AUTHZ_EXTENSION, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }
  /**
   * Paimon publishes no paimon-spark artifact for every Spark version and Scala version this
   * connector supports, so some builds bind no Paimon catalog. Registering the Paimon session
   * extension there would fail SparkSession construction, so the plugin must skip it whenever the
   * binding is absent.
   */
  @Test
  void testPaimonExtensionIsSkippedWithoutAPaimonBinding() {
    assertFalse(registeredExtensions(withoutPaimon()).contains(paimonExtension()));
  }

  /**
   * The same skip, reached through the opt-in flag rather than by calling the guarded method
   * directly. This is the composition that matters in production: a user carries
   * enablePaimonSupport=true onto a build that bound no Paimon catalog, and the flag must not be
   * enough to queue the extension.
   */
  @Test
  void testThePaimonFlagAloneDoesNotQueueTheExtensionWithoutABinding() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, "true");
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(withoutPaimon());

    plugin.registerOptInExtensions(sparkConf);
    plugin.registerSqlExtensions(sparkConf);

    String extensions = sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
    assertFalse(extensions.contains(paimonExtension()), extensions);
  }

  /**
   * A user may set spark.sql.extensions themselves. The plugin has to merge its own extensions into
   * that value rather than replace it, so seed a foreign extension: seeding only the one the plugin
   * is about to add cannot tell merging apart from overwriting.
   */
  @Test
  void testMergesIntoExtensionsTheUserAlreadySet() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), USER_EXTENSION + COMMA + AUTHZ_EXTENSION);

    new GravitinoDriverPlugin(withoutPaimon()).registerSqlExtensions(sparkConf);

    String extensions = sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
    assertTrue(extensions.contains(USER_EXTENSION), extensions);
    assertEquals(
        1,
        Arrays.stream(extensions.split(COMMA)).filter(AUTHZ_EXTENSION::equals).count(),
        extensions);
  }

  /** The catalog every provider resolves to comes from the bindings, not from a fixed table. */
  @Test
  void testEveryProviderResolvesToTheClassBoundForItsKind() {
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(withPaimon());

    assertEquals("org.example.HiveCatalog", plugin.catalogClassName("hive"));
    assertEquals("org.example.IcebergCatalog", plugin.catalogClassName("lakehouse-iceberg"));
    assertEquals(PAIMON_CATALOG, plugin.catalogClassName("lakehouse-paimon"));
    assertEquals("org.example.GlueCatalog", plugin.catalogClassName("glue"));
    assertEquals("org.example.PostgreSqlCatalog", plugin.catalogClassName("jdbc-postgresql"));
    // Every other JDBC backend shares one catalog.
    assertEquals("org.example.JdbcCatalog", plugin.catalogClassName("jdbc-mysql"));
    assertEquals("org.example.JdbcCatalog", plugin.catalogClassName("jdbc-doris"));
  }

  @Test
  void testAProviderThisBuildHasNoCatalogForResolvesToNothing() {
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(withoutPaimon());

    // Gravitino providers this connector implements no catalog for.
    Assertions.assertNull(plugin.catalogClassName("kafka"));
    Assertions.assertNull(plugin.catalogClassName("lakehouse-hudi"));
    // Paimon, on a build that bound no Paimon catalog.
    Assertions.assertNull(plugin.catalogClassName("lakehouse-paimon"));
  }

  @Test
  void testRegisteringACatalogWritesTheBoundClassUnderItsName() {
    SparkConf sparkConf = new SparkConf(false);

    new GravitinoDriverPlugin(withoutPaimon()).registerCatalog(sparkConf, "my_hive", "hive");

    assertEquals("org.example.HiveCatalog", sparkConf.get("spark.sql.catalog.my_hive"));
  }

  @Test
  void testACatalogThisBuildCannotServeIsNotRegistered() {
    SparkConf sparkConf = new SparkConf(false);
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(withoutPaimon());

    plugin.registerCatalog(sparkConf, "my_paimon", "lakehouse-paimon");
    plugin.registerCatalog(sparkConf, "my_kafka", "kafka");
    // A blank provider is skipped by its own branch, ahead of the kind lookup. null is the shape
    // that pins that branch: without it, the kind lookup dereferences the provider and NPEs, so
    // removing the branch fails here. The empty string covers the other blank shape, and asserting
    // that nothing at all was written also catches a future edit that writes some unrelated key.
    plugin.registerCatalog(sparkConf, "no_provider", "");
    plugin.registerCatalog(sparkConf, "null_provider", null);

    assertEquals(0, sparkConf.getAll().length, Arrays.toString(sparkConf.getAll()));
  }

  /**
   * Iceberg and Paimon are opt-in, in two respects: a catalog of either kind must not be registered
   * unless its flag is set, and neither one's session extensions may be queued. Both gates sit
   * above the methods the other tests call, so this goes through registerOptInExtensions and
   * registerGravitinoCatalogs, with the flags set through the conf keys users actually write, so a
   * key wired to the wrong flag fails here.
   */
  @Test
  void testAnOptInCatalogAndItsExtensionsAreRegisteredOnlyWhenItsFlagIsSet() {
    for (boolean iceberg : new boolean[] {false, true}) {
      for (boolean paimon : new boolean[] {false, true}) {
        SparkConf sparkConf = new SparkConf(false);
        sparkConf.set(
            GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, String.valueOf(iceberg));
        sparkConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, String.valueOf(paimon));
        GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(withPaimon());
        plugin.registerOptInExtensions(sparkConf);

        plugin.registerGravitinoCatalogs(
            sparkConf,
            ImmutableMap.of(
                "my_iceberg",
                catalogWithProvider("lakehouse-iceberg"),
                "my_paimon",
                catalogWithProvider("lakehouse-paimon"),
                "my_hive",
                catalogWithProvider("hive"),
                // A catalog whose provider the server left unset must be skipped, not NPE out of
                // the loop and fail SparkContext creation.
                "no_provider",
                catalogWithProvider(null)));
        plugin.registerSqlExtensions(sparkConf);

        String flags = "iceberg=" + iceberg + " paimon=" + paimon;
        assertEquals(iceberg, sparkConf.contains("spark.sql.catalog.my_iceberg"), flags);
        assertEquals(paimon, sparkConf.contains("spark.sql.catalog.my_paimon"), flags);
        // A catalog behind no flag is registered either way.
        assertTrue(sparkConf.contains("spark.sql.catalog.my_hive"), flags);
        assertFalse(sparkConf.contains("spark.sql.catalog.no_provider"), flags);

        String extensions = sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
        assertEquals(
            iceberg,
            extensions.contains(GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS),
            flags + " -> " + extensions);
        assertEquals(
            iceberg,
            extensions.contains(GravitinoIcebergSparkSessionExtensions.class.getName()),
            flags + " -> " + extensions);
        assertEquals(
            paimon,
            extensions.contains(GravitinoDriverPlugin.PAIMON_SPARK_EXTENSIONS),
            flags + " -> " + extensions);
      }
    }
  }

  private static Catalog catalogWithProvider(String provider) {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.provider()).thenReturn(provider);
    return catalog;
  }

  private static String registeredExtensions(SparkBindings bindings) {
    SparkConf sparkConf = new SparkConf(false);
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(bindings);
    plugin.registerPaimonExtensionsIfSupported();
    plugin.registerSqlExtensions(sparkConf);
    return sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
  }

  private static String paimonExtension() {
    return GravitinoDriverPlugin.PAIMON_SPARK_EXTENSIONS;
  }

  private static SparkBindings withoutPaimon() {
    return requiredCatalogs().build();
  }

  private static SparkBindings withPaimon() {
    return requiredCatalogs().catalog(SparkCatalogKind.LAKEHOUSE_PAIMON, PAIMON_CATALOG).build();
  }

  private static SparkBindings.Builder requiredCatalogs() {
    return SparkBindings.builder()
        .authorizationExtension(AUTHZ_EXTENSION)
        .catalog(SparkCatalogKind.HIVE, "org.example.HiveCatalog")
        .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, "org.example.IcebergCatalog")
        .catalog(SparkCatalogKind.GLUE, "org.example.GlueCatalog")
        .catalog(SparkCatalogKind.JDBC, "org.example.JdbcCatalog")
        .catalog(SparkCatalogKind.JDBC_POSTGRESQL, "org.example.PostgreSqlCatalog");
  }

  @Test
  void testTokenAuthTypeBuildsClient() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    // The client cannot reach a server here, but it must get past auth configuration first: an
    // unsupported auth type or a missing token would fail before any connection is attempted.
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertFalse(e instanceof UnsupportedOperationException, e.toString());
    Assertions.assertFalse(e instanceof IllegalArgumentException, e.toString());
  }

  @Test
  void testTokenProviderReturnsBearerToken() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    assertEquals(
        "Bearer a-token",
        new String(
            new DynamicBearerTokenProvider(sparkConf).getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenIsResolvedOnEveryRequest() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "first-token");
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer first-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "second-token");

    assertEquals(
        "Bearer second-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenFileTakesPrecedenceAndIsRereadEveryRequest(@TempDir Path tempDir)
      throws IOException {
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, "file-token\n".getBytes(StandardCharsets.UTF_8));
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "conf-token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE, tokenFile.toString());
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer file-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    Files.write(tokenFile, "rotated-token\n".getBytes(StandardCharsets.UTF_8));

    assertEquals(
        "Bearer rotated-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenAuthTypeWithoutTokenFails() {
    SparkConf sparkConf = tokenAuthConf();

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE));
  }

  private static SparkConf tokenAuthConf() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.TOKEN_AUTH_TYPE);
    return sparkConf;
  }
}
