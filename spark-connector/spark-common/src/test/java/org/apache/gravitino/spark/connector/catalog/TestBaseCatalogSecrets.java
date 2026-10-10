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
package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretMaterial;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBaseCatalogSecrets {

  private GravitinoClient gravitinoClient;
  private CapturingCatalog catalog;

  @BeforeAll
  void initCatalogManager() {
    gravitinoClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testMergeSecrets() {
    setUpCatalog(Map.of("metastore.uris", "thrift://localhost:9083"), Map.of("s3-sk", "secret-sk"));

    catalog.initialize("hive", new CaseInsensitiveStringMap(Map.of()));

    assertEquals("thrift://localhost:9083", catalog.lastProperties.get("metastore.uris"));
    assertEquals("secret-sk", catalog.lastProperties.get("s3-sk"));
  }

  @Test
  void testMergeSecretsNullProps() {
    setUpCatalog(null, Map.of("s3-sk", "secret-sk"));

    catalog.initialize("hive", new CaseInsensitiveStringMap(Map.of()));

    assertEquals("secret-sk", catalog.lastProperties.get("s3-sk"));
    assertEquals(1, catalog.lastProperties.size());
  }

  @Test
  void testMergeMemorySecrets() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>();
      entityProps.put("custom-token", "placeholder");
      List<SecretMaterial> writes =
          sm.assembleSecretMaterials(
              Map.of(),
              entityProps,
              "catalog",
              1L,
              Map.of("custom-token", new SecretBinding("memory", "from-memory")),
              Map.of());
      sm.writeSecrets(writes);
      Map<String, String> secrets = SecretPropertyUtils.buildSecrets(sm, entityProps);

      setUpCatalog(
          Map.of("jdbc-url", "jdbc:mysql://localhost/db"),
          secrets,
          new Credential[] {new JdbcCredential("root", "jdbc-pwd")});
      catalog.initialize("jdbc", new CaseInsensitiveStringMap(Map.of()));

      assertEquals("jdbc:mysql://localhost/db", catalog.lastProperties.get("jdbc-url"));
      assertEquals("from-memory", catalog.lastProperties.get("custom-token"));
      assertEquals("jdbc-pwd", catalog.lastProperties.get("jdbc-password"));
    }
  }

  static Stream<RuntimeException> getCredentialsFailures() {
    return Stream.of(
        new RESTException("transient failure"),
        new NotFoundException("credentials endpoint missing"));
  }

  @Test
  void testGetSecretsRestExceptionAbortsInitialize() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    TableCatalog sparkCatalog = mock(TableCatalog.class);
    when(gravitinoCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(gravitinoCatalog.provider()).thenReturn("hive");
    when(gravitinoCatalog.properties())
        .thenReturn(Map.of("metastore.uris", "thrift://localhost:9083"));
    when(gravitinoCatalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenThrow(new RESTException("transient /secrets failure"));
    when(gravitinoClient.loadCatalog(any())).thenReturn(gravitinoCatalog);
    GravitinoCatalogManager.get().close();
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
    catalog = new CapturingCatalog(sparkCatalog);

    assertThrows(
        RESTException.class,
        () -> catalog.initialize("hive", new CaseInsensitiveStringMap(Map.of())));
  }

  @ParameterizedTest
  @MethodSource("getCredentialsFailures")
  void testGetCredentialsFailureDoesNotAbortInitialize(RuntimeException failure) {
    Catalog gravitinoCatalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    TableCatalog sparkCatalog = mock(TableCatalog.class);
    when(gravitinoCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(gravitinoCatalog.provider()).thenReturn("hive");
    when(gravitinoCatalog.properties())
        .thenReturn(Map.of("metastore.uris", "thrift://localhost:9083"));
    when(gravitinoCatalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenReturn(Map.of());
    when(gravitinoCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenThrow(failure);
    when(gravitinoClient.loadCatalog(any())).thenReturn(gravitinoCatalog);
    GravitinoCatalogManager.get().close();
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
    catalog = new CapturingCatalog(sparkCatalog);

    catalog.initialize("hive", new CaseInsensitiveStringMap(Map.of()));

    assertEquals("thrift://localhost:9083", catalog.lastProperties.get("metastore.uris"));
  }

  @Test
  void testSkipsExpiringCredentialsFromGetCredentials() {
    setUpCatalog(
        Map.of("s3-endpoint", "http://s3.example.com"),
        Map.of(),
        new Credential[] {
          new S3TokenCredential("SESSION", "session-secret", "tok", 1_700_000_000_000L),
          new S3SecretKeyCredential("AKIATEST", "static-secret")
        });

    catalog.initialize("hive", new CaseInsensitiveStringMap(Map.of()));

    assertEquals("AKIATEST", catalog.lastProperties.get("s3-access-key-id"));
    assertEquals("static-secret", catalog.lastProperties.get("s3-secret-access-key"));
    assertFalse(catalog.lastProperties.containsKey("s3-session-token"));
    assertEquals("http://s3.example.com", catalog.lastProperties.get("s3-endpoint"));
  }

  private void setUpCatalog(Map<String, String> properties, Map<String, String> secrets) {
    setUpCatalog(properties, secrets, new Credential[0]);
  }

  private void setUpCatalog(
      Map<String, String> properties, Map<String, String> secrets, Credential[] credentials) {
    Catalog gravitinoCatalog = mock(Catalog.class);
    SupportsSecrets supportsSecrets = mock(SupportsSecrets.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    TableCatalog sparkCatalog = mock(TableCatalog.class);
    when(gravitinoCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(gravitinoCatalog.provider()).thenReturn("hive");
    when(gravitinoCatalog.properties()).thenReturn(properties);
    when(gravitinoCatalog.supportsSecrets()).thenReturn(supportsSecrets);
    when(supportsSecrets.getSecrets()).thenReturn(secrets);
    when(gravitinoCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(credentials);
    when(gravitinoClient.loadCatalog(any())).thenReturn(gravitinoCatalog);
    // Catalog info is cached; recreate the manager so each test loads the new mock.
    GravitinoCatalogManager.get().close();
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
    catalog = new CapturingCatalog(sparkCatalog);
  }

  private static SecretManager memorySecretManager() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    config.loadFromProperties(properties);
    return new SecretManager(config);
  }

  private static class CapturingCatalog extends BaseCatalog {
    private final TableCatalog backing;
    private Map<String, String> lastProperties = Map.of();

    private CapturingCatalog(TableCatalog backing) {
      this.backing = backing;
    }

    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      this.lastProperties = Map.copyOf(properties);
      return backing;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return sparkTable;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return mock(PropertiesConverter.class);
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return mock(SparkTransformConverter.class);
    }
  }
}
