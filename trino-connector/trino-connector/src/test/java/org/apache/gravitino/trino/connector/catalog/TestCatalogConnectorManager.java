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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClientAuthenticationConfig;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCatalogConnectorManager {

  @Test
  public void testSingleMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    assertEquals("memory", manager.getTrinoCatalogName("test", "memory"));
  }

  @Test
  public void testMultiMetalakeCatalogNaming() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    assertEquals("\"test.memory\"", manager.getTrinoCatalogName("test", "memory"));
  }

  @Test
  public void testSingleMetalakeRejectsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "true"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    TrinoException error =
        assertThrows(
            TrinoException.class,
            () ->
                manager.createCatalogConnectorContext(
                    "test1", otherConnectorConfig, mockContext()));
    assertEquals(GravitinoErrorCode.GRAVITINO_OPERATION_FAILED.toErrorCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("Failed to create connector"));
    assertTrue(error.getCause().getMessage().contains("Multiple metalakes are not supported"));
  }

  @Test
  public void testMultiMetalakeAllowsDifferentMetalakeConnector() throws Exception {
    CatalogConnectorFactory catalogFactory = createCatalogConnectorFactory();
    CatalogConnectorManager manager =
        createManager(
            catalogFactory,
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test",
                "gravitino.use-single-metalake",
                "false"));

    GravitinoConfig connectorConfig = createConnectorConfig(catalogConfigJson("test", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test0", connectorConfig, mockContext()));

    GravitinoConfig otherConnectorConfig =
        createConnectorConfig(catalogConfigJson("test2", "memory"));
    assertDoesNotThrow(
        () -> manager.createCatalogConnectorContext("test1", otherConnectorConfig, mockContext()));
  }

  @Test
  public void testSkipCatalogPatterns() throws Exception {
    CatalogConnectorManager manager =
        createManager(
            ImmutableMap.of(
                "gravitino.uri",
                "http://127.0.0.1:8090",
                "gravitino.metalake",
                "test1",
                "gravitino.trino.skip-catalog-patterns",
                "a.*, b1"));

    assertTrue(manager.skipCatalog("a1"));
    assertTrue(manager.skipCatalog("b1"));
    assertFalse(manager.skipCatalog("b2"));
  }

  @Test
  public void testBuildGravitinoClientNoAuth() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertDoesNotThrow(() -> manager.config(buildConfig(ImmutableMap.of()), null));
  }

  @Test
  public void testBuildGravitinoClientNoneAuth() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertDoesNotThrow(
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "none")),
                null));
  }

  @Test
  public void testBuildGravitinoClientSimpleAuthWithUser() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertDoesNotThrow(
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "simple",
                        GravitinoClientAuthenticationConfig.SIMPLE_AUTH_USER_KEY, "alice")),
                null));
  }

  @Test
  public void testBuildGravitinoClientSimpleAuthNoUser() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertDoesNotThrow(
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "simple")),
                null));
  }

  @Test
  public void testBuildGravitinoClientInvalidAuthType() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertThrows(
        TrinoException.class,
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "invalid_type")),
                null));
  }

  @Test
  public void testBuildGravitinoClientOAuthMissingServerUri() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertThrows(
        TrinoException.class,
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth",
                        GravitinoClientAuthenticationConfig.OAUTH_CREDENTIAL_KEY, "cred",
                        GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY, "oauth2/token",
                        GravitinoClientAuthenticationConfig.OAUTH_SCOPE_KEY, "scope")),
                null));
  }

  @Test
  public void testBuildGravitinoClientOAuthMissingCredential() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertThrows(
        TrinoException.class,
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "oauth",
                        GravitinoClientAuthenticationConfig.OAUTH_SERVER_URI_KEY,
                            "http://auth.example.com",
                        GravitinoClientAuthenticationConfig.OAUTH_PATH_KEY, "oauth2/token",
                        GravitinoClientAuthenticationConfig.OAUTH_SCOPE_KEY, "scope")),
                null));
  }

  @Test
  public void testBuildGravitinoClientKerberosMissingPrincipal() {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertThrows(
        TrinoException.class,
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos")),
                null));
  }

  @Test
  public void testBuildGravitinoClientKerberosKeytabNotFound(@TempDir java.nio.file.Path tempDir) {
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertThrows(
        TrinoException.class,
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
                        GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY,
                            "user@REALM.COM",
                        GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY,
                            tempDir.resolve("missing.keytab").toString())),
                null));
  }

  @Test
  public void testBuildGravitinoClientKerberosWithKeytab(@TempDir java.nio.file.Path tempDir)
      throws IOException {
    File keytabFile = tempDir.resolve("user.keytab").toFile();
    Files.write(keytabFile.toPath(), new byte[0]);
    CatalogConnectorManager manager = createManagerWithNullClient(ImmutableMap.of());
    assertDoesNotThrow(
        () ->
            manager.config(
                buildConfig(
                    ImmutableMap.of(
                        GravitinoClientAuthenticationConfig.AUTH_TYPE_KEY, "kerberos",
                        GravitinoClientAuthenticationConfig.KERBEROS_PRINCIPAL_KEY,
                            "user@REALM.COM",
                        GravitinoClientAuthenticationConfig.KERBEROS_KEYTAB_FILE_PATH_KEY,
                            keytabFile.getAbsolutePath())),
                null));
  }

  private CatalogConnectorManager createManagerWithNullClient(
      ImmutableMap<String, String> ignored) {
    CatalogRegister catalogRegister = mock(CatalogRegister.class);
    CatalogConnectorFactory catalogFactory;
    try {
      catalogFactory = createCatalogConnectorFactory();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new CatalogConnectorManager(catalogRegister, catalogFactory, null);
  }

  private GravitinoConfig buildConfig(ImmutableMap<String, String> authConfig) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("gravitino.uri", "http://127.0.0.1:8090")
            .put("gravitino.metalake", "test");
    builder.putAll(authConfig);
    return new GravitinoConfig(builder.build());
  }

  private CatalogConnectorManager createManager(ImmutableMap<String, String> configMap)
      throws Exception {
    return createManager(createCatalogConnectorFactory(), configMap);
  }

  private CatalogConnectorManager createManager(
      CatalogConnectorFactory catalogFactory, ImmutableMap<String, String> configMap) {
    CatalogRegister catalogRegister = mock(CatalogRegister.class);

    boolean singleMetalakeMode =
        configMap.getOrDefault("gravitino.use-single-metalake", "true").equals("true");
    CatalogConnectorManager manager =
        new CatalogConnectorManager(
            catalogRegister,
            catalogFactory,
            singleMetalakeMode
                ? null
                : (metalake, catalog) -> String.format("\"%s.%s\"", metalake, catalog));
    manager.config(new GravitinoConfig(configMap), mock(GravitinoAdminClient.class));
    return manager;
  }

  private CatalogConnectorFactory createCatalogConnectorFactory() throws Exception {
    CatalogConnectorFactory catalogFactory = mock(CatalogConnectorFactory.class);
    CatalogConnectorContext.Builder builder = mock(CatalogConnectorContext.Builder.class);
    when(catalogFactory.createCatalogConnectorContextBuilder(any())).thenReturn(builder);
    when(builder.withMetalake(any())).thenReturn(builder);
    when(builder.withContext(any())).thenReturn(builder);
    when(builder.build()).thenReturn(mock(CatalogConnectorContext.class));
    return catalogFactory;
  }

  private static GravitinoConfig createConnectorConfig(String catalogConfigJson) {
    return new GravitinoConfig(
        ImmutableMap.of(
            "gravitino.uri",
            "http://127.0.0.1:8090",
            "gravitino.metalake",
            "test",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR,
            "true",
            GravitinoConfig.GRAVITINO_DYNAMIC_CONNECTOR_CATALOG_CONFIG,
            catalogConfigJson));
  }

  private static String catalogConfigJson(String metalake, String name) throws Exception {
    GravitinoCatalog catalog =
        new GravitinoCatalog(metalake, "memory", name, ImmutableMap.of(), 0L);
    return GravitinoCatalog.toJson(catalog);
  }

  private static ConnectorContext mockContext() {
    return mock(ConnectorContext.class);
  }
}
