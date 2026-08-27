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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin;
import org.apache.spark.SparkConf;
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
        String uri, String advertisedCatalogName) {
      return ImmutableMap.of("impl", "rest", "uri", uri, "parent", advertisedCatalogName);
    }

    @Override
    public String[] sparkExtensions() {
      return new String[] {Runnable.class.getName()};
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
        String uri, String advertisedCatalogName) {
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
}
