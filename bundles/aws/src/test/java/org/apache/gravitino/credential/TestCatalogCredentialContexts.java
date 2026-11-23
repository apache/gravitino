/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.credential;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.s3.credential.S3TokenProvider;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Exercises the real dispatcher, providers and S3 generator, mocking only catalog paths and STS.
 */
public class TestCatalogCredentialContexts {
  private static final NameIdentifier CATALOG = NameIdentifier.of("metalake", "catalog");
  private static final NameIdentifier FILESET =
      NameIdentifier.of("metalake", "catalog", "schema", "fileset");
  private static final String PATH = "s3://bucket/warehouse/table";

  /** Catalog probes have no authorized storage paths and must not request an STS token. */
  @Test
  public void testTokenOnlyCatalogProbeReturnsNoCredentials() {
    try (Fixture fixture = new Fixture(false)) {
      Assertions.assertInstanceOf(
          S3TokenProvider.class,
          fixture.manager.getCredentialProvider(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE).get());
      Assertions.assertFalse(fixture.manager.hasStaticCredentialProviders());
      Assertions.assertTrue(
          fixture
              .dispatcher
              .getCredentials(fixture.catalog, CATALOG, CredentialPrivilege.READ)
              .isEmpty());
      verifyNoInteractions(fixture.sts);
    }
  }

  /** Authorized mixed catalog requests return static credentials, without inventing token paths. */
  @Test
  public void testMixedCatalogProbeReturnsOnlyStaticCredentials() {
    try (Fixture fixture = new Fixture(true)) {
      Assertions.assertTrue(fixture.manager.hasStaticCredentialProviders());
      List<Credential> credentials =
          fixture.dispatcher.getCredentials(fixture.catalog, CATALOG, CredentialPrivilege.READ);
      Assertions.assertEquals(1, credentials.size());
      S3SecretKeyCredential credential =
          Assertions.assertInstanceOf(S3SecretKeyCredential.class, credentials.get(0));
      Assertions.assertEquals("source-id", credential.accessKeyId());
      Assertions.assertEquals("source-key", credential.secretAccessKey());
      verifyNoInteractions(fixture.sts);
    }
  }

  /**
   * Token generation works when the dispatcher supplies the existing authorized path context.
   *
   * @param privilege the authorized fileset storage privilege
   */
  @ParameterizedTest
  @EnumSource(CredentialPrivilege.class)
  public void testPathCredentialsReachRealS3Generator(CredentialPrivilege privilege) {
    try (Fixture fixture = new Fixture(false)) {
      Assertions.assertTrue(
          fixture.dispatcher.getCredentials(fixture.catalog, CATALOG, privilege).isEmpty());
      verifyNoInteractions(fixture.sts);

      List<Credential> credentials =
          fixture.dispatcher.getCredentials(fixture.catalog, FILESET, privilege);
      Assertions.assertEquals(1, credentials.size());
      S3TokenCredential credential =
          Assertions.assertInstanceOf(S3TokenCredential.class, credentials.get(0));
      Assertions.assertEquals("session-id", credential.accessKeyId());
      Assertions.assertEquals("session-token", credential.sessionToken());
      ArgumentCaptor<AssumeRoleRequest> request = ArgumentCaptor.forClass(AssumeRoleRequest.class);
      verify(fixture.sts).assumeRole(request.capture());
      String policy = request.getValue().policy();
      Assertions.assertTrue(policy.contains("arn:aws:s3:::bucket/warehouse/table/*"), policy);
      Assertions.assertFalse(policy.contains("arn:aws:s3:::bucket/*"), policy);
      Assertions.assertTrue(policy.contains("s3:GetObject"), policy);
      Assertions.assertEquals(
          privilege == CredentialPrivilege.WRITE, policy.contains("s3:PutObject"), policy);
    }
  }

  private static class Fixture implements AutoCloseable {
    private final StsClient sts = mock(StsClient.class);
    private final CatalogCredentialManager manager;
    private final BaseCatalog<?> catalog = mock(BaseCatalog.class);
    private final CredentialOperationDispatcher dispatcher =
        new CredentialOperationDispatcher(
            mock(CatalogManager.class), mock(EntityStore.class),
            mock(IdGenerator.class), mock(SecretManager.class));

    private Fixture(boolean mixed) {
      Map<String, String> properties =
          Map.of(
              CredentialConstants.CREDENTIAL_PROVIDERS,
              mixed ? "s3-token,s3-secret-key" : "s3-token",
              S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
              "source-id",
              S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
              "source-key",
              S3Properties.GRAVITINO_S3_REGION,
              "us-east-1",
              S3Properties.GRAVITINO_S3_ROLE_ARN,
              "arn:aws:iam::123456789012:role/vending");
      StsClientBuilder builder = mock(StsClientBuilder.class, Answers.RETURNS_SELF);
      when(builder.build()).thenReturn(sts);
      when(sts.assumeRole(any(AssumeRoleRequest.class)))
          .thenReturn(
              AssumeRoleResponse.builder()
                  .credentials(
                      Credentials.builder()
                          .accessKeyId("session-id")
                          .secretAccessKey("session-key")
                          .sessionToken("session-token")
                          .expiration(Instant.now().plusSeconds(3600))
                          .build())
                  .build());
      try (MockedStatic<StsClient> clients = mockStatic(StsClient.class)) {
        clients.when(StsClient::builder).thenReturn(builder);
        manager = new CatalogCredentialManager("catalog", properties);
      }
      when(catalog.catalogCredentialManager()).thenReturn(manager);
      when(catalog.propertiesWithCredentialProviders()).thenReturn(properties);
      CatalogOperations ops =
          mock(
              CatalogOperations.class,
              Mockito.withSettings().extraInterfaces(SupportsPathBasedCredentials.class));
      when(((SupportsPathBasedCredentials) ops).getPathContext(any()))
          .thenReturn(List.of(new PathContext(PATH, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE)));
      when(catalog.ops()).thenReturn(ops);
    }

    /** Closes the real credential providers and their SDK client. */
    @Override
    public void close() {
      manager.close();
    }
  }
}
