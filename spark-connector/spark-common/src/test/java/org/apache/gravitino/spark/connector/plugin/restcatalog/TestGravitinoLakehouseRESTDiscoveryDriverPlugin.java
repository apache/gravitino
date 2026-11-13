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

package org.apache.gravitino.spark.connector.plugin.restcatalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.gravitino.spark.connector.plugin.restcatalog.lance.LanceRESTCatalogProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoLakehouseRESTDiscoveryDriverPlugin {

  private static final String DISCOVERY_PLUGIN =
      GravitinoLakehouseRESTDiscoveryPlugin.class.getName();
  private static final String GRAVITINO_PLUGIN = GravitinoSparkPlugin.class.getName();
  private static final String URI_CONFIG = "spark.sql.gravitino.fakeREST.uri";
  private static final String CATALOG_PREFIX = "spark.sql.catalog.";

  @BeforeEach
  void resetPolicies() {
    TrackingPolicy.invocationCount = 0;
  }

  @Test
  void testRequiresDiscoveryPluginBeforeGravitinoPlugin() {
    SparkConf sparkConf =
        baseConf().set("spark.plugins", GRAVITINO_PLUGIN + "," + DISCOVERY_PLUGIN);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("must be listed before"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a"));
  }

  @Test
  void testNoConfiguredUriDoesNotChangeSparkConf() {
    SparkConf sparkConf =
        new SparkConf(false).set("spark.plugins", DISCOVERY_PLUGIN + "," + GRAVITINO_PLUGIN);

    driver().initialize(sparkConf, providerClasses("fake", FailingProvider.class));

    assertFalse(sparkConf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testGeneratedConfigurationAndPrecedence() {
    SparkConf sparkConf =
        baseConf()
            .set("spark.sql.gravitino.fakeREST.catalogProperties.impl", "global-impl")
            .set("spark.sql.gravitino.fakeREST.catalogProperties.uri", "global-uri")
            .set("spark.sql.gravitino.fakeREST.catalogProperties.extra", "global-extra")
            .set(CATALOG_PREFIX + "catalog_a.uri", "user-uri")
            .set(
                StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
                "example.UserExtension,java.lang.Runnable");

    initialize(sparkConf);

    assertEquals(String.class.getName(), sparkConf.get(CATALOG_PREFIX + "catalog_a"));
    assertEquals("rest", sparkConf.get(CATALOG_PREFIX + "catalog_a.impl"));
    assertEquals("user-uri", sparkConf.get(CATALOG_PREFIX + "catalog_a.uri"));
    assertEquals("catalog_a", sparkConf.get(CATALOG_PREFIX + "catalog_a.parent"));
    assertEquals("global-extra", sparkConf.get(CATALOG_PREFIX + "catalog_a.extra"));
    assertEquals(
        "java.lang.Runnable,example.UserExtension",
        sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testLanceReusesSparkContextUser() {
    SparkConf sparkConf = new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri");
    SparkContext context = mock(SparkContext.class);
    when(context.conf()).thenReturn(sparkConf);
    when(context.sparkUser()).thenReturn("etuser");
    GravitinoLakehouseRESTDiscoveryDriverPlugin plugin =
        new GravitinoLakehouseRESTDiscoveryDriverPlugin() {
          @Override
          void initialize(
              SparkConf conf, Map<String, String> providers, @Nullable String sparkUser) {
            super.initialize(conf, providerClasses("lance", LanceAuthProvider.class), sparkUser);
          }
        };

    plugin.init(context, null);

    assertEquals("Basic ZXR1c2VyOnVudXNlZA==", LanceAuthProvider.discoveryAuthorization);
    assertEquals(
        LanceAuthProvider.discoveryAuthorization,
        sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
  }

  @Test
  void testLanceUsesSparkUserForDiscoveryAndCatalogs() {
    SparkConf sparkConf = new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri");

    driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

    assertEquals("Basic ZXR1c2VyOnVudXNlZA==", LanceAuthProvider.discoveryAuthorization);
    assertEquals(
        LanceAuthProvider.discoveryAuthorization,
        sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
  }

  @Test
  void testLanceUsesSparkUserWithExplicitSimpleAuthentication() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "simple");

    driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

    assertEquals("Basic ZXR1c2VyOnVudXNlZA==", LanceAuthProvider.discoveryAuthorization);
    assertEquals(
        LanceAuthProvider.discoveryAuthorization,
        sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
  }

  @Test
  void testLanceDoesNotGenerateAuthorizationForOtherAuthenticationTypes() {
    for (String authType : Arrays.asList("basic", "oauth2", "kerberos", "token", "unknown", "")) {
      SparkConf sparkConf =
          new SparkConf(false)
              .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
              .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, authType);

      driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

      assertNull(LanceAuthProvider.discoveryAuthorization);
      assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
    }
  }

  @Test
  void testLancePreservesExplicitAuthorizationForOtherAuthenticationTypes() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "oauth2")
            .set(
                "spark.sql.gravitino.lanceREST.catalogProperties.headers.Authorization",
                "Bearer discovery-token")
            .set(CATALOG_PREFIX + "catalog_a.headers.Authorization", "Bearer catalog-token");

    driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

    assertEquals("Bearer discovery-token", LanceAuthProvider.discoveryAuthorization);
    assertEquals(
        "Bearer catalog-token", sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
  }

  @Test
  void testLancePreservesExplicitAuthorization() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
            .set(
                "spark.sql.gravitino.lanceREST.catalogProperties.headers.authorization",
                "Bearer discovery-token")
            .set(CATALOG_PREFIX + "catalog_a.headers.AUTHORIZATION", "Bearer catalog-token");

    driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

    assertEquals("Bearer discovery-token", LanceAuthProvider.discoveryAuthorization);
    assertEquals(
        "Bearer catalog-token", sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.headers.authorization"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.headers.AUTHORIZATION"));
  }

  @Test
  void testLanceDiscoveryUsesSparkUserWithKerberosEnabled() throws IOException {
    AtomicReference<List<String>> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/lance/v1/namespace/$/list",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().get("Authorization"));
          byte[] body = "{\"namespaces\":[\"catalog_a\"]}".getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    Configuration kerberos = new Configuration(false);
    kerberos.set("hadoop.security.authentication", "kerberos");
    String realmProperty = "java.security.krb5.realm";
    String kdcProperty = "java.security.krb5.kdc";
    String originalRealm = System.getProperty(realmProperty);
    String originalKdc = System.getProperty(kdcProperty);
    try {
      System.setProperty(realmProperty, "EXAMPLE.COM");
      System.setProperty(kdcProperty, "localhost");
      UserGroupInformation.setConfiguration(kerberos);
      assertTrue(UserGroupInformation.isSecurityEnabled());
      SparkConf sparkConf =
          new SparkConf(false)
              .set(
                  "spark.sql.gravitino.lanceREST.uri",
                  "http://127.0.0.1:" + server.getAddress().getPort() + "/lance");

      driver().initialize(sparkConf, providerClasses("lance", HttpLanceProvider.class), "etuser");

      assertEquals(Collections.singletonList("Basic ZXR1c2VyOnVudXNlZA=="), authorization.get());
      assertEquals(
          authorization.get().get(0),
          sparkConf.get(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
    } finally {
      UserGroupInformation.reset();
      restoreSystemProperty(realmProperty, originalRealm);
      restoreSystemProperty(kdcProperty, originalKdc);
      server.stop(0);
    }
  }

  @Test
  void testLanceCollapsesDuplicateHeadersAndPreservesCatalogOverrides() {
    String global = "spark.sql.gravitino.lanceREST.catalogProperties.";
    String catalog = CATALOG_PREFIX + "catalog_a.";
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
            .set(global + "headers.Authorization", "Bearer discovery")
            .set(global + "Headers.authorization", "Bearer discovery")
            .set(global + "headers.X-Global", "global")
            .set(global + "headers.x-global", "global")
            .set(catalog + "headers.authorization", "Bearer catalog")
            .set(catalog + "headers.AUTHORIZATION", "Bearer catalog")
            .set(catalog + "Headers.X-User", "catalog");

    driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser");

    assertEquals("Bearer discovery", LanceAuthProvider.discoveryAuthorization);
    assertEquals("Bearer catalog", sparkConf.get(catalog + "headers.Authorization"));
    assertEquals("global", sparkConf.get(catalog + "headers.x-global"));
    assertEquals("catalog", sparkConf.get(catalog + "headers.x-user"));
    assertFalse(sparkConf.contains(catalog + "headers.authorization"));
    assertFalse(sparkConf.contains(catalog + "headers.AUTHORIZATION"));
    assertFalse(sparkConf.contains(catalog + "headers.X-Global"));
    assertFalse(sparkConf.contains(catalog + "Headers.X-User"));
  }

  @Test
  void testLanceRejectsConflictingHeadersBeforeRegisteringCatalogs() {
    for (String prefix :
        Arrays.asList(
            "spark.sql.gravitino.lanceREST.catalogProperties.", CATALOG_PREFIX + "catalog_a.")) {
      SparkConf sparkConf =
          new SparkConf(false)
              .set("spark.sql.gravitino.lanceREST.uri", "rest-uri")
              .set(prefix + "headers.Authorization", "secret-one")
              .set(prefix + "headers.authorization", "secret-two");
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  driver()
                      .initialize(
                          sparkConf, providerClasses("lance", LanceAuthProvider.class), "etuser"));
      assertEquals(
          "Conflicting Lance HTTP header values for headers.Authorization", error.getMessage());
      assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a"));
    }
  }

  @Test
  void testLanceRejectsColonInAutomaticallyEncodedUser() {
    SparkConf sparkConf = new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri");

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(
                        sparkConf, providerClasses("lance", LanceAuthProvider.class), "a:b"));

    assertTrue(error.getMessage().contains("must not contain ':'"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a"));
  }

  @Test
  void testLanceWithoutSparkUserDoesNotGenerateAuthorization() {
    for (String userName : Arrays.asList(null, "", "  ")) {
      SparkConf sparkConf =
          new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri");

      driver().initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), userName);

      assertNull(LanceAuthProvider.discoveryAuthorization);
      assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
    }
  }

  @Test
  void testSparkUserDoesNotChangeOtherFormats() {
    SparkConf sparkConf = baseConf();

    driver().initialize(sparkConf, providerClasses("fake", FakeProvider.class), "etuser");

    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.headers.Authorization"));
  }

  @Test
  void testLanceEncodesSparkUserAsUtf8() {
    SparkConf sparkConf = new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri");

    driver()
        .initialize(sparkConf, providerClasses("lance", LanceAuthProvider.class), "\u7528\u6237");

    assertEquals("Basic 55So5oi3OnVudXNlZA==", LanceAuthProvider.discoveryAuthorization);
  }

  @Test
  void testUserOwnedCatalogDoesNotReachPolicy() {
    SparkConf sparkConf =
        baseConf()
            .set(CATALOG_PREFIX + "catalog_a", "example.UserCatalog")
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                TrackingPolicy.class.getName());

    initialize(sparkConf);

    assertEquals(0, TrackingPolicy.invocationCount);
    assertEquals("example.UserCatalog", sparkConf.get(CATALOG_PREFIX + "catalog_a"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a.parent"));
  }

  @Test
  void testPolicyFiltersAndRenamesCatalogs() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                RenamePolicy.class.getName());

    driver().initialize(sparkConf, providerClasses("fake", MultipleCatalogProvider.class));

    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a"));
    assertEquals(String.class.getName(), sparkConf.get(CATALOG_PREFIX + "renamed_b"));
    assertEquals("catalog_b", sparkConf.get(CATALOG_PREFIX + "renamed_b.parent"));
  }

  @Test
  void testDuplicatePolicyOutputDoesNotPartiallyModifySparkConf() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                DuplicatePolicy.class.getName());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(sparkConf, providerClasses("fake", MultipleCatalogProvider.class)));

    assertTrue(exception.getMessage().contains("duplicate name"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "duplicate"));
    assertFalse(sparkConf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testInvalidPolicyOutputFails() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                InvalidNamePolicy.class.getName());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("invalid Spark identifier"));
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "invalid-name"));
  }

  @Test
  void testInvalidDiscoveredNameIsSkippedUnderDefaultPolicy() {
    SparkConf sparkConf = baseConf();

    driver().initialize(sparkConf, providerClasses("fake", InvalidNameProvider.class));

    assertFalse(sparkConf.contains(CATALOG_PREFIX + "my-catalog"));
    assertEquals(String.class.getName(), sparkConf.get(CATALOG_PREFIX + "catalog_a"));
    assertEquals("catalog_a", sparkConf.get(CATALOG_PREFIX + "catalog_a.parent"));
    assertEquals(
        Runnable.class.getName(), sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testBlankPolicyOutputFails() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                BlankNamePolicy.class.getName());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("returned a blank catalog name"));
  }

  @Test
  void testConfiguredFormatWithoutProviderFails() {
    SparkConf sparkConf = baseConf();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> driver().initialize(sparkConf, Collections.emptyMap()));

    assertTrue(exception.getMessage().contains("No lakehouse REST catalog provider"));
  }

  @Test
  void testBuiltinProviderClassNames() {
    Map<String, String> providerClassNames = BuiltinRESTCatalogProviders.providerClassNames();

    assertEquals(1, providerClassNames.size());
    assertEquals(
        "org.apache.gravitino.spark.connector.plugin.restcatalog.lance.LanceRESTCatalogProvider",
        providerClassNames.get("lance"));
  }

  @Test
  void testMissingProviderClassFails() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(
                        baseConf(),
                        Collections.singletonMap("fake", "example.MissingRESTCatalogProvider")));

    assertTrue(exception.getMessage().contains("Failed to instantiate"));
    assertTrue(exception.getCause() instanceof ClassNotFoundException);
  }

  @Test
  void testProviderClassMustImplementProviderInterface() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(
                        baseConf(), Collections.singletonMap("fake", String.class.getName())));

    assertTrue(exception.getMessage().contains("does not implement"));
    assertTrue(exception.getMessage().contains(LakehouseRESTCatalogProvider.class.getName()));
  }

  @Test
  void testMisspelledUriKeysAreReported() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.plugins", DISCOVERY_PLUGIN)
            .set("spark.sql.gravitino.lanceRest.uri", "rest-uri")
            .set("spark.sql.gravitino.REST.uri", "rest-uri")
            .set("spark.sql.gravitino.uri", "http://127.0.0.1:8090")
            .set("spark.sql.gravitino.lanceREST.catalogProperties.namespaceDelimiter", "#");

    List<String> similarKeys =
        GravitinoLakehouseRESTDiscoveryDriverPlugin.similarUriKeys(sparkConf);

    assertEquals(2, similarKeys.size());
    assertTrue(similarKeys.contains("spark.sql.gravitino.lanceRest.uri"));
    assertTrue(similarKeys.contains("spark.sql.gravitino.REST.uri"));
  }

  @Test
  void testMatchingUriKeyIsNotReportedAsMisspelled() {
    assertTrue(
        GravitinoLakehouseRESTDiscoveryDriverPlugin.similarUriKeys(
                new SparkConf(false).set("spark.sql.gravitino.lanceREST.uri", "rest-uri"))
            .isEmpty());
  }

  @Test
  void testMissingCatalogClassFails() {
    SparkConf sparkConf = baseConf();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(
                        sparkConf, providerClasses("fake", MissingCatalogClassProvider.class)));

    assertTrue(exception.getMessage().contains("Required runtime class"));
    assertTrue(exception.getMessage().contains(MissingCatalogClassProvider.CATALOG_CLASS));
    assertTrue(exception.getCause() instanceof ClassNotFoundException);
    assertFalse(sparkConf.contains(CATALOG_PREFIX + "catalog_a"));
  }

  @Test
  void testMissingSparkExtensionClassFails() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(
                        baseConf(), providerClasses("fake", MissingExtensionProvider.class)));

    assertTrue(exception.getMessage().contains("Required runtime class"));
    assertTrue(exception.getMessage().contains(MissingExtensionProvider.EXTENSION_CLASS));
  }

  @Test
  void testBlankCatalogClassFails() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                driver()
                    .initialize(
                        baseConf(), providerClasses("fake", BlankCatalogClassProvider.class)));

    assertTrue(exception.getMessage().contains("returned a blank runtime class"));
  }

  @Test
  void testNullSparkExtensionsFails() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                driver()
                    .initialize(baseConf(), providerClasses("fake", NullExtensionsProvider.class)));

    assertTrue(exception.getMessage().contains("returned null Spark extensions"));
  }

  @Test
  void testProviderListingFailureReportsFormatAndUri() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                driver()
                    .initialize(baseConf(), providerClasses("fake", FailingListProvider.class)));

    assertTrue(exception.getMessage().contains("Failed to list fake REST catalogs from rest-uri"));
    assertTrue(exception.getCause().getMessage().contains("listing is unavailable"));
  }

  @Test
  void testBlankPolicyClassNameFails() {
    SparkConf sparkConf =
        baseConf().set(GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG, " ");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(
        exception
            .getMessage()
            .contains(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG
                    + " must not be blank"));
  }

  @Test
  void testMissingPolicyClassFails() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                "example.MissingCatalogRegistrationPolicy");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("Failed to instantiate"));
    assertTrue(
        exception
            .getMessage()
            .contains(GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG));
    assertTrue(exception.getCause() instanceof ClassNotFoundException);
  }

  @Test
  void testPolicyClassMustImplementPolicyInterface() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                String.class.getName());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("does not implement"));
    assertTrue(exception.getMessage().contains(CatalogRegistrationPolicy.class.getName()));
  }

  @Test
  void testPolicyWithoutNoArgConstructorFails() {
    SparkConf sparkConf =
        baseConf()
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                NoArgConstructorMissingPolicy.class.getName());

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> initialize(sparkConf));

    assertTrue(exception.getMessage().contains("Failed to instantiate"));
    assertTrue(exception.getCause() instanceof NoSuchMethodException);
  }

  @Test
  void testPolicyReturningUserOwnedNameFails() {
    SparkConf sparkConf =
        baseConf()
            .set(CATALOG_PREFIX + "renamed_b", "example.UserCatalog")
            .set(
                GravitinoLakehouseRESTDiscoveryDriverPlugin.REGISTRATION_POLICY_CONFIG,
                RenamePolicy.class.getName());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                driver()
                    .initialize(sparkConf, providerClasses("fake", MultipleCatalogProvider.class)));

    assertTrue(exception.getMessage().contains("which is configured by the user"));
    assertEquals("example.UserCatalog", sparkConf.get(CATALOG_PREFIX + "renamed_b"));
  }

  private static SparkConf baseConf() {
    return new SparkConf(false).set("spark.plugins", DISCOVERY_PLUGIN).set(URI_CONFIG, "rest-uri");
  }

  private static GravitinoLakehouseRESTDiscoveryDriverPlugin driver() {
    return new GravitinoLakehouseRESTDiscoveryDriverPlugin();
  }

  private static void initialize(SparkConf sparkConf) {
    driver().initialize(sparkConf, providerClasses("fake", FakeProvider.class));
  }

  private static Map<String, String> providerClasses(
      String format, Class<? extends LakehouseRESTCatalogProvider> providerClass) {
    return Collections.singletonMap(format, providerClass.getName());
  }

  private static void restoreSystemProperty(String key, @Nullable String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  static class FakeProvider implements LakehouseRESTCatalogProvider {

    /** Creates the fake provider. */
    public FakeProvider() {}

    @Override
    public String format() {
      return "fake";
    }

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      return Collections.singletonList("catalog_a");
    }

    @Override
    public String catalogClassName() {
      return String.class.getName();
    }

    @Override
    public Map<String, String> generatedCatalogProperties(
        String uri, String discoveredCatalogName) {
      return ImmutableMap.of("impl", "rest", "uri", uri, "parent", discoveredCatalogName);
    }

    @Override
    public String[] sparkExtensions() {
      return new String[] {Runnable.class.getName()};
    }
  }

  static class HttpLanceProvider extends LanceRESTCatalogProvider {

    /** Creates a provider that uses the real Lance HTTP client without native Spark classes. */
    public HttpLanceProvider() {}

    @Override
    public String catalogClassName() {
      return String.class.getName();
    }

    @Override
    public String[] sparkExtensions() {
      return new String[0];
    }
  }

  static class LanceAuthProvider extends HttpLanceProvider {

    private static String discoveryAuthorization;

    /** Creates the Lance provider that records discovery authentication. */
    public LanceAuthProvider() {}

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      discoveryAuthorization =
          catalogProperties.entrySet().stream()
              .filter(entry -> "headers.Authorization".equalsIgnoreCase(entry.getKey()))
              .map(Map.Entry::getValue)
              .findFirst()
              .orElse(null);
      return Collections.singletonList("catalog_a");
    }
  }

  static class MultipleCatalogProvider extends FakeProvider {

    /** Creates the provider that advertises multiple catalogs. */
    public MultipleCatalogProvider() {}

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      return Arrays.asList("catalog_a", "catalog_b");
    }
  }

  static class FailingProvider implements LakehouseRESTCatalogProvider {

    static {
      if (Boolean.parseBoolean("true")) {
        throw new AssertionError("Provider must not be loaded without a configured URI");
      }
    }

    /** Creates the provider that must never be loaded. */
    public FailingProvider() {}

    @Override
    public String format() {
      throw new AssertionError("Provider must not be loaded without a configured URI");
    }

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      throw new AssertionError("Provider must not be called without a configured URI");
    }

    @Override
    public String catalogClassName() {
      throw new AssertionError("Provider must not be called without a configured URI");
    }

    @Override
    public Map<String, String> generatedCatalogProperties(
        String uri, String discoveredCatalogName) {
      throw new AssertionError("Provider must not be called without a configured URI");
    }

    @Override
    public String[] sparkExtensions() {
      throw new AssertionError("Provider must not be called without a configured URI");
    }
  }

  /** Policy used to verify user-owned catalogs are filtered before policy invocation. */
  public static class TrackingPolicy implements CatalogRegistrationPolicy {
    private static int invocationCount;

    /** Creates the policy. */
    public TrackingPolicy() {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      invocationCount++;
      return true;
    }
  }

  /** Policy used to filter one catalog and rename another. */
  public static class RenamePolicy implements CatalogRegistrationPolicy {

    /** Creates the policy. */
    public RenamePolicy() {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      return "catalog_b".equals(catalogName);
    }

    @Override
    public String registeredCatalogName(String format, String catalogName) {
      return "renamed_b";
    }
  }

  /** Policy used to produce a duplicate registration name. */
  public static class DuplicatePolicy implements CatalogRegistrationPolicy {

    /** Creates the policy. */
    public DuplicatePolicy() {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      return true;
    }

    @Override
    public String registeredCatalogName(String format, String catalogName) {
      return "duplicate";
    }
  }

  /** Policy used to produce an invalid Spark identifier. */
  public static class InvalidNamePolicy implements CatalogRegistrationPolicy {

    /** Creates the policy. */
    public InvalidNamePolicy() {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      return true;
    }

    @Override
    public String registeredCatalogName(String format, String catalogName) {
      return "invalid-name";
    }
  }

  /** Policy that cannot be instantiated reflectively. */
  public static class NoArgConstructorMissingPolicy implements CatalogRegistrationPolicy {

    /**
     * Creates the policy.
     *
     * @param unused an argument that prevents reflective instantiation.
     */
    public NoArgConstructorMissingPolicy(String unused) {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      return true;
    }
  }

  static class MissingCatalogClassProvider extends FakeProvider {

    static final String CATALOG_CLASS = "example.MissingNamespaceSparkCatalog";

    /** Creates the provider that advertises an absent catalog class. */
    public MissingCatalogClassProvider() {}

    @Override
    public String catalogClassName() {
      return CATALOG_CLASS;
    }
  }

  static class BlankCatalogClassProvider extends FakeProvider {

    /** Creates the provider that advertises a blank catalog class. */
    public BlankCatalogClassProvider() {}

    @Override
    public String catalogClassName() {
      return " ";
    }
  }

  static class MissingExtensionProvider extends FakeProvider {

    static final String EXTENSION_CLASS = "example.MissingSparkSessionExtensions";

    /** Creates the provider that advertises an absent Spark extension. */
    public MissingExtensionProvider() {}

    @Override
    public String[] sparkExtensions() {
      return new String[] {EXTENSION_CLASS};
    }
  }

  static class NullExtensionsProvider extends FakeProvider {

    /** Creates the provider that returns null Spark extensions. */
    public NullExtensionsProvider() {}

    @Override
    public String[] sparkExtensions() {
      return null;
    }
  }

  static class InvalidNameProvider extends FakeProvider {

    /** Creates the provider that advertises a name Spark cannot reference unquoted. */
    public InvalidNameProvider() {}

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      return Arrays.asList("my-catalog", "catalog_a");
    }
  }

  /** Policy used to produce a blank registration name. */
  public static class BlankNamePolicy implements CatalogRegistrationPolicy {

    /** Creates the policy. */
    public BlankNamePolicy() {}

    @Override
    public boolean shouldRegister(String format, String catalogName) {
      return true;
    }

    @Override
    public String registeredCatalogName(String format, String catalogName) {
      return " ";
    }
  }

  static class FailingListProvider extends FakeProvider {

    /** Creates the provider whose catalog listing fails. */
    public FailingListProvider() {}

    @Override
    public List<String> listCatalogs(String uri, Map<String, String> catalogProperties) {
      throw new IllegalStateException("listing is unavailable");
    }
  }
}
