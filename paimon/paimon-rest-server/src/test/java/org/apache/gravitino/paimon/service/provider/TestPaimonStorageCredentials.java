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
package org.apache.gravitino.paimon.service.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.credentials.Client;
import com.aliyun.credentials.models.Config;
import com.aliyun.credentials.models.CredentialModel;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.credential.CredentialProvider;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.paimon.service.provider.PaimonConfigProvider.CatalogConfig;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/** Exercises built-in S3/OSS providers in both modes, replacing only cloud SDK calls. */
public class TestPaimonStorageCredentials {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testS3GeneratorUsesWarehouseWritePolicyAndNeverReturnsSourceKeys(boolean auxiliary)
      throws Exception {
    StsClient sts = mock(StsClient.class);
    StsClientBuilder builder = mock(StsClientBuilder.class, Answers.RETURNS_SELF);
    when(builder.build()).thenReturn(sts);
    long expiry = Instant.now().plusSeconds(7200).toEpochMilli();
    when(sts.assumeRole(any(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId("session-id")
                        .secretAccessKey("session-key")
                        .sessionToken("session-token")
                        .expiration(Instant.ofEpochMilli(expiry))
                        .build())
                .build());
    Map<String, String> properties =
        new HashMap<>(
            Map.of(
                "catalog-backend",
                "filesystem",
                "warehouse",
                "s3://bucket/warehouse",
                "credential-providers",
                "s3-token,s3-secret-key",
                "s3-access-key-id",
                "source-id",
                "s3-secret-access-key",
                "source-key",
                "s3-region",
                "us-east-1",
                "s3-role-arn",
                "arn:aws:iam::123456789012:role/vending",
                "s3-token-expire-in-secs",
                "7200",
                "gravitino.bypass.data-token.enabled",
                "true"));
    try (MockedStatic<StsClient> clients = mockStatic(StsClient.class)) {
      clients.when(StsClient::builder).thenReturn(builder);
      CatalogConfig config = resolve(properties, auxiliary);
      assertEquals("session-id", config.properties().get("s3.access-key"));
      assertEquals("session-token", config.properties().get("s3.session-token"));
      assertEquals(expiry, config.expiresAtMillis());
      assertFalse(config.properties().containsValue("source-key"));
      ArgumentCaptor<AssumeRoleRequest> request = ArgumentCaptor.forClass(AssumeRoleRequest.class);
      verify(sts).assumeRole(request.capture());
      assertEquals(7200, request.getValue().durationSeconds());
      assertTrue(request.getValue().policy().contains("bucket/warehouse/*"));
      assertTrue(request.getValue().policy().contains("s3:PutObject"));
      assertTrue(request.getValue().policy().contains("\"arn:aws:s3:::bucket/warehouse\""));
      assertTrue(request.getValue().policy().contains("\"warehouse\""));
      assertFalse(request.getValue().policy().contains("arn:aws:s3:::bucket/*"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testOssGeneratorUsesWarehouseWritePolicyAndActualExpiry(boolean auxiliary) throws Exception {
    long expiry = Instant.now().plusSeconds(7200).toEpochMilli();
    CredentialModel model =
        CredentialModel.builder()
            .accessKeyId("session-id")
            .accessKeySecret("session-key")
            .securityToken("session-token")
            .expiration(expiry)
            .build();
    AtomicReference<Config> request = new AtomicReference<>();
    try (MockedConstruction<Client> clients =
        mockConstruction(
            Client.class,
            (client, context) -> {
              request.set((Config) context.arguments().get(0));
              when(client.getCredential()).thenReturn(model);
            })) {
      CatalogConfig config =
          resolve(
              Map.of(
                  "catalog-backend",
                  "filesystem",
                  "warehouse",
                  "oss://bucket/warehouse",
                  "credential-providers",
                  "oss-token",
                  "oss-access-key-id",
                  "source-id",
                  "oss-secret-access-key",
                  "source-key",
                  "oss-role-arn",
                  "acs:ram::123456789012:role/vending",
                  "oss-region",
                  "cn-hangzhou",
                  "oss-token-expire-in-secs",
                  "7200"),
              auxiliary);
      assertEquals("session-id", config.properties().get("fs.oss.accessKeyId"));
      assertEquals("session-token", config.properties().get("fs.oss.securityToken"));
      assertEquals(expiry, config.expiresAtMillis());
      assertFalse(config.properties().containsValue("source-key"));
      assertTrue(request.get().getPolicy().contains("bucket/warehouse/*"));
      assertTrue(request.get().getPolicy().contains("oss:PutObject"));
      assertEquals(7200, request.get().getRoleSessionExpiration());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testStsFailureCannotFallBackToTheSourceKeys(boolean auxiliary) {
    StsClient sts = mock(StsClient.class);
    StsClientBuilder builder = mock(StsClientBuilder.class, Answers.RETURNS_SELF);
    when(builder.build()).thenReturn(sts);
    when(sts.assumeRole(any(AssumeRoleRequest.class)))
        .thenThrow(new IllegalStateException("STS unavailable"));
    try (MockedStatic<StsClient> clients = mockStatic(StsClient.class)) {
      clients.when(StsClient::builder).thenReturn(builder);
      assertThrows(
          RuntimeException.class,
          () ->
              resolve(
                  Map.of(
                      "catalog-backend",
                      "filesystem",
                      "warehouse",
                      "s3://bucket/warehouse",
                      "credential-providers",
                      "s3-token,s3-secret-key",
                      "s3-access-key-id",
                      "source-id",
                      "s3-secret-access-key",
                      "source-key",
                      "s3-region",
                      "us-east-1",
                      "s3-role-arn",
                      "arn:aws:iam::123456789012:role/vending"),
                  auxiliary));
      verify(sts).assumeRole(any(AssumeRoleRequest.class));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testEmptyTokenAndInvalidWarehouseCannotReturnStaticKeys(boolean auxiliary) throws Exception {
    CredentialProvider tokenProvider = mock(CredentialProvider.class);
    when(tokenProvider.supportsScheme("s3")).thenReturn(true);
    try (MockedConstruction<CatalogCredentialManager> managers =
        mockConstruction(
            CatalogCredentialManager.class,
            (manager, context) -> {
              when(manager.getCredentialProvider("s3-token"))
                  .thenReturn(Optional.of(tokenProvider));
              when(manager.getCredential(any(), any())).thenReturn(Optional.empty());
            })) {
      Map<String, String> properties =
          new HashMap<>(
              Map.of(
                  "catalog-backend",
                  "filesystem",
                  "warehouse",
                  "s3://bucket/warehouse",
                  "credential-providers",
                  "s3-token,s3-secret-key",
                  "s3-access-key-id",
                  "source-id",
                  "s3-secret-access-key",
                  "source-key"));
      assertThrows(ServiceUnavailableException.class, () -> resolve(properties, auxiliary));
      for (String invalid :
          List.of("file:/tmp/warehouse", "s3://bucket/a/../b", "s3://bucket/warehouse?x=1")) {
        properties.put("warehouse", invalid);
        assertThrows(IllegalArgumentException.class, () -> resolve(properties, auxiliary));
      }
    }
  }

  private CatalogConfig resolve(Map<String, String> properties, boolean auxiliary)
      throws Exception {
    if (!auxiliary) {
      GravitinoClient client = mock(GravitinoClient.class);
      Catalog catalog = mock(Catalog.class);
      when(client.loadCatalog("sales")).thenReturn(catalog);
      when(catalog.name()).thenReturn("sales");
      when(catalog.provider()).thenReturn("lakehouse-paimon");
      when(catalog.properties()).thenReturn(properties);
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(catalog.supportsCredentials()).thenReturn(credentials);
      when(credentials.getCredentials()).thenReturn(new Credential[0]);
      try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(client)) {
        provider.initialize(Map.of("gravitino-uri", "http://localhost:1"));
        return provider.getCatalogConfig("sales").orElseThrow();
      }
    }
    properties = new HashMap<>(properties);
    properties.put(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX, "true");
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogs = mock(CatalogManager.class);
    BaseCatalog<?> catalog = mock(BaseCatalog.class);
    when(catalog.name()).thenReturn("sales");
    when(catalog.provider()).thenReturn("lakehouse-paimon");
    when(catalog.propertiesWithCredentialProviders()).thenReturn(properties);
    when(env.catalogManager()).thenReturn(catalogs);
    CredentialOperationDispatcher credentials = mock(CredentialOperationDispatcher.class);
    when(env.credentialOperationDispatcher()).thenReturn(credentials);
    when(credentials.getCredentials(any(), any())).thenReturn(List.of());
    when(catalogs.doWithCatalog(eq(NameIdentifier.of("default_metalake", "sales")), any()))
        .thenAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, ?> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            });
    try (CatalogCredentialManager manager = new CatalogCredentialManager("sales", properties);
        MockedStatic<GravitinoEnv> environments = mockStatic(GravitinoEnv.class)) {
      environments.when(GravitinoEnv::getInstance).thenReturn(env);
      when(catalog.catalogCredentialManager()).thenReturn(manager);
      try (DynamicPaimonConfigProvider provider = new DynamicPaimonConfigProvider(true)) {
        provider.initialize(Map.of());
        return provider.getCatalogConfig("sales").orElseThrow();
      }
    }
  }
}
