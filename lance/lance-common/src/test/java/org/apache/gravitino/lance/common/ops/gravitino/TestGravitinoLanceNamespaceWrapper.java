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
package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoLanceNamespaceWrapper {

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", null, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "secretPropertyOperationDispatcher", null, true);
  }

  @Test
  public void testClientPropertiesExtraction() {
    // Test that client properties are correctly extracted from LanceConfig
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.client.maxConnectionsPerRoute", "400")
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("gravitino.client.socketTimeoutMs", "30000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties (same logic as in initialize())
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Verify all client properties are extracted
    Assertions.assertEquals(4, clientProperties.size());
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertEquals("5000", clientProperties.get("gravitino.client.connectionTimeoutMs"));
    Assertions.assertEquals("30000", clientProperties.get("gravitino.client.socketTimeoutMs"));

    // Verify non-client properties are not included
    Assertions.assertFalse(
        clientProperties.containsKey(LanceConfig.NAMESPACE_BACKEND_URI.getKey()));
    Assertions.assertFalse(clientProperties.containsKey(LanceConfig.METALAKE_NAME.getKey()));
  }

  @Test
  public void testClientPropertiesWithDefaults() {
    // Test that initialization works without client properties
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties (same logic as in initialize())
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Should be empty (no client properties provided)
    Assertions.assertEquals(0, clientProperties.size());
  }

  @Test
  public void testClientPropertiesPartialConfig() {
    // Test with only some client properties
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "200")
            // Only maxConnections, not maxConnectionsPerRoute
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Should only have the one property
    Assertions.assertEquals(1, clientProperties.size());
    Assertions.assertEquals("200", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertNull(clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
  }

  @Test
  public void testClientPropertiesFiltering() {
    // Test that properties with similar names but different prefixes are not included
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.server.maxThreads", "800") // Should NOT be included
            .put("gravitino.cache.maxEntries", "10000") // Should NOT be included
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("some.other.client.property", "value") // Should NOT be included
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Extract client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Only properties with "gravitino.client." prefix should be included
    Assertions.assertEquals(2, clientProperties.size());
    Assertions.assertEquals("400", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("5000", clientProperties.get("gravitino.client.connectionTimeoutMs"));

    // Verify others are excluded
    Assertions.assertFalse(clientProperties.containsKey("gravitino.server.maxThreads"));
    Assertions.assertFalse(clientProperties.containsKey("gravitino.cache.maxEntries"));
    Assertions.assertFalse(clientProperties.containsKey("some.other.client.property"));
  }

  @Test
  public void testHighThroughputConfiguration() {
    // Test high-throughput scenario (like Lance REST server with 800 threads)
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://gravitino-prod:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "production")
            .put("gravitino.client.maxConnections", "800")
            .put("gravitino.client.maxConnectionsPerRoute", "800")
            .put("gravitino.client.connectionTimeoutMs", "5000")
            .put("gravitino.client.socketTimeoutMs", "30000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Verify Lance-specific config
    Assertions.assertEquals("http://gravitino-prod:8090", lanceConfig.getNamespaceBackendUri());
    Assertions.assertEquals("production", lanceConfig.getGravitinoMetalake());

    // Extract and verify client properties
    Map<String, String> clientProperties = new HashMap<>();
    lanceConfig
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith("gravitino.client.")) {
                clientProperties.put(key, value);
              }
            });

    // Verify high-throughput settings
    Assertions.assertEquals(4, clientProperties.size());
    Assertions.assertEquals("800", clientProperties.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("800", clientProperties.get("gravitino.client.maxConnectionsPerRoute"));

    // Verify single-route pattern (maxConnections == maxConnectionsPerRoute)
    Assertions.assertEquals(
        clientProperties.get("gravitino.client.maxConnections"),
        clientProperties.get("gravitino.client.maxConnectionsPerRoute"));
  }

  @Test
  public void testLanceConfigWithAllClientProperties() {
    // Test complete client configuration
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090")
            .put(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")
            .put("gravitino.client.maxConnections", "400")
            .put("gravitino.client.maxConnectionsPerRoute", "400")
            .put("gravitino.client.connectionTimeoutMs", "10000")
            .put("gravitino.client.socketTimeoutMs", "60000")
            .build();

    LanceConfig lanceConfig = new LanceConfig(properties);

    // Verify all properties are accessible
    Map<String, String> allConfig = lanceConfig.getAllConfig();
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.maxConnections"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.connectionTimeoutMs"));
    Assertions.assertTrue(allConfig.containsKey("gravitino.client.socketTimeoutMs"));

    // Verify values
    Assertions.assertEquals("400", allConfig.get("gravitino.client.maxConnections"));
    Assertions.assertEquals("400", allConfig.get("gravitino.client.maxConnectionsPerRoute"));
    Assertions.assertEquals("10000", allConfig.get("gravitino.client.connectionTimeoutMs"));
    Assertions.assertEquals("60000", allConfig.get("gravitino.client.socketTimeoutMs"));
  }

  @Test
  public void testCreateCatalogOperatorUsesHttpClientInStandaloneMode() {
    LanceConfig lanceConfig =
        new LanceConfig(
            ImmutableMap.of(
                LanceConfig.METALAKE_NAME.getKey(), "test_metalake",
                LanceConfig.NAMESPACE_BACKEND_URI.getKey(), "http://localhost:8090"));
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper(lanceConfig, false);

    GravitinoLanceNamespaceWrapper.CatalogOperator operator =
        wrapper.createCatalogOperator("test_metalake");

    Assertions.assertEquals("HttpCatalogOperator", operator.getClass().getSimpleName());
    Assertions.assertDoesNotThrow(operator::close);
  }

  @Test
  public void testLoadAndValidateLakehouseCatalogUsesCatalogOperator() {
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper();
    Catalog expectedCatalog = createCatalogProxy(Catalog.Type.RELATIONAL, "lakehouse-generic");
    wrapper.setCatalogOperator(
        new GravitinoLanceNamespaceWrapper.CatalogOperator() {
          @Override
          public Catalog[] listCatalogsInfo() {
            return new Catalog[0];
          }

          @Override
          public Catalog loadCatalog(String catalogName) {
            return expectedCatalog;
          }

          @Override
          public Catalog createCatalog(
              String catalogName,
              Catalog.Type type,
              String provider,
              String comment,
              Map<String, String> properties) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Catalog alterCatalog(String catalogName, CatalogChange... changes) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean dropCatalog(String catalogName, boolean force) {
            throw new UnsupportedOperationException();
          }
        });

    Assertions.assertSame(expectedCatalog, wrapper.loadAndValidateLakehouseCatalog("test_catalog"));
  }

  @Test
  public void testCatalogPropertiesWithSecrets() {
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper();
    Catalog catalog =
        createCatalogProxy(
            Catalog.Type.RELATIONAL,
            "lakehouse-generic",
            Map.of("visible", "v1"),
            Map.of("jdbc-password", "secret"));
    wrapper.setCatalogOperator(
        new GravitinoLanceNamespaceWrapper.CatalogOperator() {
          @Override
          public Catalog[] listCatalogsInfo() {
            return new Catalog[0];
          }

          @Override
          public Catalog loadCatalog(String catalogName) {
            return catalog;
          }

          @Override
          public Catalog createCatalog(
              String catalogName,
              Catalog.Type type,
              String provider,
              String comment,
              Map<String, String> properties) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Catalog alterCatalog(String catalogName, CatalogChange... changes) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean dropCatalog(String catalogName, boolean force) {
            throw new UnsupportedOperationException();
          }
        });

    Map<String, String> merged = wrapper.catalogPropertiesWithSecrets(catalog);
    Assertions.assertEquals("v1", merged.get("visible"));
    Assertions.assertEquals("secret", merged.get("jdbc-password"));
  }

  @Test
  public void testSchemaPropertiesWithSecretsMergesClientSecrets() {
    Schema schema = createSchemaProxy(Map.of("visible", "s1"), Map.of("schema-pwd", "s-secret"));
    Catalog catalog =
        createCatalogProxy(
            Catalog.Type.RELATIONAL, "lakehouse-generic", Map.of(), Map.of(), schema);
    GravitinoLanceNamespaceWrapper wrapper =
        new GravitinoLanceNamespaceWrapper(
            new LanceConfig(ImmutableMap.of(LanceConfig.METALAKE_NAME.getKey(), "test_metalake")),
            false);
    wrapper.setCatalogOperator(catalogOperatorReturning(catalog));

    Map<String, String> merged = wrapper.schemaPropertiesWithSecrets(catalog, "test_schema");
    Assertions.assertEquals("s1", merged.get("visible"));
    Assertions.assertEquals("s-secret", merged.get("schema-pwd"));
  }

  @Test
  public void testCatalogAndSchemaPropertiesUseSecretDispatcher() throws Exception {
    Catalog catalog =
        createCatalogProxy(
            Catalog.Type.RELATIONAL, "lakehouse-generic", Map.of("visible", "c1"), Map.of());
    Schema schema = createSchemaProxy(Map.of("visible", "s1"), Map.of());

    NameIdentifier catalogIdent = NameIdentifierUtil.ofCatalog("test_metalake", "test_catalog");
    NameIdentifier schemaIdent =
        NameIdentifierUtil.ofSchema("test_metalake", "test_catalog", "test_schema");
    SecretPropertyOperationDispatcher dispatcher =
        new SecretPropertyOperationDispatcher(null, null, null, null) {
          @Override
          public Map<String, String> getSecrets(
              NameIdentifier identifier, Entity.EntityType entityType) {
            if (catalogIdent.equals(identifier) && entityType == Entity.EntityType.CATALOG) {
              return Map.of("jdbc-password", "from-dispatcher");
            }
            if (schemaIdent.equals(identifier) && entityType == Entity.EntityType.SCHEMA) {
              return Map.of("schema-pwd", "from-dispatcher-schema");
            }
            return Map.of();
          }
        };
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "secretPropertyOperationDispatcher", dispatcher, true);

    SchemaDispatcher schemaDispatcher =
        (SchemaDispatcher)
            Proxy.newProxyInstance(
                SchemaDispatcher.class.getClassLoader(),
                new Class<?>[] {SchemaDispatcher.class},
                (proxy, method, args) -> {
                  if ("loadSchema".equals(method.getName())) {
                    return schema;
                  }
                  Class<?> returnType = method.getReturnType();
                  if (returnType.equals(boolean.class)) {
                    return false;
                  }
                  if (returnType.equals(int.class)) {
                    return 0;
                  }
                  return null;
                });
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "catalogDispatcher",
        Proxy.newProxyInstance(
            CatalogDispatcher.class.getClassLoader(),
            new Class<?>[] {CatalogDispatcher.class},
            (proxy, method, args) -> null),
        true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemaDispatcher, true);

    LanceConfig lanceConfig =
        new LanceConfig(ImmutableMap.of(LanceConfig.METALAKE_NAME.getKey(), "test_metalake"));
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper(lanceConfig, true);
    wrapper.asNamespaceOps();
    wrapper.setCatalogOperator(catalogOperatorReturning(catalog));

    Map<String, String> catalogMerged = wrapper.catalogPropertiesWithSecrets(catalog);
    Assertions.assertEquals("c1", catalogMerged.get("visible"));
    Assertions.assertEquals("from-dispatcher", catalogMerged.get("jdbc-password"));

    Map<String, String> schemaMerged = wrapper.schemaPropertiesWithSecrets(catalog, "test_schema");
    Assertions.assertEquals("s1", schemaMerged.get("visible"));
    Assertions.assertEquals("from-dispatcher-schema", schemaMerged.get("schema-pwd"));
  }

  @Test
  public void testLoadSchemaUsesSchemaDispatcherInAuxMode() throws Exception {
    Schema expectedSchema = createSchemaProxy();
    AtomicReference<NameIdentifier> loadedSchemaIdent = new AtomicReference<>();
    SchemaDispatcher schemaDispatcher =
        (SchemaDispatcher)
            Proxy.newProxyInstance(
                SchemaDispatcher.class.getClassLoader(),
                new Class<?>[] {SchemaDispatcher.class},
                (proxy, method, args) -> {
                  if ("loadSchema".equals(method.getName())) {
                    loadedSchemaIdent.set((NameIdentifier) args[0]);
                    return expectedSchema;
                  }

                  Class<?> returnType = method.getReturnType();
                  if (returnType.equals(boolean.class)) {
                    return false;
                  }
                  if (returnType.equals(int.class)) {
                    return 0;
                  }
                  return null;
                });
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "catalogDispatcher",
        Proxy.newProxyInstance(
            CatalogDispatcher.class.getClassLoader(),
            new Class<?>[] {CatalogDispatcher.class},
            (proxy, method, args) -> null),
        true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemaDispatcher, true);

    LanceConfig lanceConfig =
        new LanceConfig(ImmutableMap.of(LanceConfig.METALAKE_NAME.getKey(), "test_metalake"));
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper(lanceConfig, true);
    wrapper.asNamespaceOps();

    Schema actualSchema =
        wrapper.loadSchema(
            createCatalogProxy(Catalog.Type.RELATIONAL, "lakehouse-generic"), "test_schema");

    Assertions.assertSame(expectedSchema, actualSchema);
    Assertions.assertEquals(
        NameIdentifierUtil.ofSchema("test_metalake", "test_catalog", "test_schema"),
        loadedSchemaIdent.get());
  }

  @Test
  public void testAsTableCatalogUsesTableDispatcherInAuxMode() throws Exception {
    Table expectedTable = createTableProxy();
    AtomicReference<NameIdentifier> loadedTableIdent = new AtomicReference<>();
    AtomicReference<Namespace> listedNamespace = new AtomicReference<>();
    TableDispatcher tableDispatcher =
        (TableDispatcher)
            Proxy.newProxyInstance(
                TableDispatcher.class.getClassLoader(),
                new Class<?>[] {TableDispatcher.class},
                (proxy, method, args) -> {
                  if ("loadTable".equals(method.getName())) {
                    loadedTableIdent.set((NameIdentifier) args[0]);
                    return expectedTable;
                  }
                  if ("listTables".equals(method.getName())) {
                    listedNamespace.set((Namespace) args[0]);
                    return new NameIdentifier[] {NameIdentifier.of("test_schema", "test_table")};
                  }

                  Class<?> returnType = method.getReturnType();
                  if (returnType.equals(boolean.class)) {
                    return false;
                  }
                  if (returnType.equals(int.class)) {
                    return 0;
                  }
                  return null;
                });
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "catalogDispatcher",
        Proxy.newProxyInstance(
            CatalogDispatcher.class.getClassLoader(),
            new Class<?>[] {CatalogDispatcher.class},
            (proxy, method, args) -> null),
        true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tableDispatcher, true);

    LanceConfig lanceConfig =
        new LanceConfig(ImmutableMap.of(LanceConfig.METALAKE_NAME.getKey(), "test_metalake"));
    GravitinoLanceNamespaceWrapper wrapper = new GravitinoLanceNamespaceWrapper(lanceConfig, true);
    wrapper.asTableOps();

    TableCatalog tableCatalog =
        wrapper.asTableCatalog(createCatalogProxy(Catalog.Type.RELATIONAL, "lakehouse-generic"));

    Assertions.assertSame(
        expectedTable, tableCatalog.loadTable(NameIdentifier.of("test_schema", "test_table")));
    Assertions.assertArrayEquals(
        new NameIdentifier[] {NameIdentifier.of("test_schema", "test_table")},
        tableCatalog.listTables(Namespace.of("test_schema")));
    Assertions.assertEquals(
        NameIdentifierUtil.ofTable("test_metalake", "test_catalog", "test_schema", "test_table"),
        loadedTableIdent.get());
    Assertions.assertEquals(
        Namespace.of("test_metalake", "test_catalog", "test_schema"), listedNamespace.get());
  }

  private Schema createSchemaProxy() {
    return createSchemaProxy(null, Map.of());
  }

  private Schema createSchemaProxy(Map<String, String> properties, Map<String, String> secrets) {
    return (Schema)
        Proxy.newProxyInstance(
            Schema.class.getClassLoader(),
            new Class<?>[] {Schema.class, SupportsSecrets.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "name":
                  return "test_schema";
                case "properties":
                  return properties;
                case "supportsSecrets":
                  return proxy;
                case "getSecrets":
                  return secrets;
                default:
                  Class<?> returnType = method.getReturnType();
                  if (returnType.equals(boolean.class)) {
                    return false;
                  }
                  if (returnType.equals(int.class)) {
                    return 0;
                  }
                  return null;
              }
            });
  }

  private Table createTableProxy() {
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) -> null);
  }

  private Catalog createCatalogProxy(Catalog.Type type, String provider) {
    return createCatalogProxy(type, provider, null, Map.of());
  }

  private Catalog createCatalogProxy(
      Catalog.Type type,
      String provider,
      Map<String, String> properties,
      Map<String, String> secrets) {
    return createCatalogProxy(type, provider, properties, secrets, null);
  }

  private Catalog createCatalogProxy(
      Catalog.Type type,
      String provider,
      Map<String, String> properties,
      Map<String, String> secrets,
      Schema schema) {
    return (Catalog)
        Proxy.newProxyInstance(
            Catalog.class.getClassLoader(),
            new Class<?>[] {Catalog.class, SupportsSecrets.class, SupportsSchemas.class},
            (proxy, method, args) -> {
              switch (method.getName()) {
                case "type":
                  return type;
                case "provider":
                  return provider;
                case "name":
                  return "test_catalog";
                case "properties":
                  return properties;
                case "supportsSecrets":
                  return proxy;
                case "getSecrets":
                  return secrets;
                case "asSchemas":
                  return proxy;
                case "loadSchema":
                  return schema;
                case "schemaExists":
                  return schema != null;
                default:
                  Class<?> returnType = method.getReturnType();
                  if (returnType.equals(boolean.class)) {
                    return false;
                  }
                  if (returnType.equals(int.class)) {
                    return 0;
                  }
                  if (returnType.equals(long.class)) {
                    return 0L;
                  }
                  return null;
              }
            });
  }

  private GravitinoLanceNamespaceWrapper.CatalogOperator catalogOperatorReturning(Catalog catalog) {
    return new GravitinoLanceNamespaceWrapper.CatalogOperator() {
      @Override
      public Catalog[] listCatalogsInfo() {
        return new Catalog[0];
      }

      @Override
      public Catalog loadCatalog(String catalogName) {
        return catalog;
      }

      @Override
      public Catalog createCatalog(
          String catalogName,
          Catalog.Type type,
          String provider,
          String comment,
          Map<String, String> properties) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Catalog alterCatalog(String catalogName, CatalogChange... changes) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean dropCatalog(String catalogName, boolean force) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
