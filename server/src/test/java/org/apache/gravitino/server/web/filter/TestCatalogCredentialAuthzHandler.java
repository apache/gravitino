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
package org.apache.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.DlfSecretKeyCredential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.dto.credential.CredentialDTO;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

/** Tests catalog credential response authorization. */
public class TestCatalogCredentialAuthzHandler {

  /** Checks that each token type is available without static-secret privileges. */
  @ParameterizedTest
  @MethodSource("tokenCredentials")
  public void testCatalogTokenCredentials(Credential token) {
    assertCredentialsForSecretPrivileges(
        Collections.singletonList(token), Collections.singletonList(token));
  }

  /** Checks that every static credential type still requires both privileges. */
  @ParameterizedTest
  @MethodSource("staticCredentials")
  public void testCatalogStaticCredentials(Credential credential) {
    assertCredentialsForSecretPrivileges(
        Collections.singletonList(credential), Collections.emptyList());
  }

  /** Checks that mixed results never include static credentials without both privileges. */
  @ParameterizedTest
  @MethodSource("staticCredentials")
  public void testCatalogMixedCredentials(Credential credential) {
    List<Credential> tokens = tokenCredentials().collect(Collectors.toList());
    List<Credential> credentials = new ArrayList<>(tokens);
    credentials.add(0, credential);
    assertCredentialsForSecretPrivileges(credentials, tokens);
  }

  /** Checks that an empty catalog does not require secret privileges. */
  @Test
  public void testEmptyCredentials() {
    assertCredentialsForSecretPrivileges(Collections.emptyList(), Collections.emptyList());
  }

  /** Checks that error and unrelated responses retain their status and payload. */
  @Test
  public void testPreservesOtherResponses() {
    try (MockedStatic<MetadataAuthzHelper> auth = mockStatic(MetadataAuthzHelper.class)) {
      CatalogCredentialAuthzHandler handler = createHandler();
      Object value = new Object();
      Assertions.assertSame(value, handler.processResponse(value));
      for (int status : new int[] {200, 400, 403, 500}) {
        try (Response response = Response.status(status).entity("unchanged").build()) {
          Assertions.assertSame(response, handler.processResponse(response));
        }
      }
      try (Response response =
          Response.status(500).entity(new CredentialResponse(new CredentialDTO[0])).build()) {
        Assertions.assertSame(response, handler.processResponse(response));
      }
    }
  }

  /** Checks that filtering preserves response headers and the original credentials. */
  @Test
  public void testPreservesHeadersAndOriginalResponse() {
    List<Credential> credentials =
        Arrays.asList(
            new S3SecretKeyCredential("id", "key"),
            new S3TokenCredential("id", "key", "token", Long.MAX_VALUE));
    try (MockedStatic<MetadataAuthzHelper> auth = mockStatic(MetadataAuthzHelper.class)) {
      CatalogCredentialAuthzHandler handler = createHandler();
      try (Response original =
              Response.fromResponse(credentialResponse(credentials))
                  .header("X-Test", "value")
                  .build();
          Response response = (Response) handler.processResponse(original)) {
        assertCredentials(response, credentials.subList(1, 2));
        Assertions.assertEquals("value", response.getHeaderString("X-Test"));
        Assertions.assertEquals(
            2, ((CredentialResponse) original.getEntity()).getCredentials().length);
      }
    }
  }

  private static Stream<Credential> tokenCredentials() {
    return Stream.of(
        new S3TokenCredential("access-id", "secret-key", "token", Long.MAX_VALUE),
        new OSSTokenCredential("access-id", "secret-key", "token", Long.MAX_VALUE),
        new ADLSTokenCredential("account", "sas-token", Long.MAX_VALUE),
        new GCSTokenCredential("token", Long.MAX_VALUE),
        new AwsIrsaCredential("access-id", "secret-key", "token", Long.MAX_VALUE),
        new COSTokenCredential("access-id", "secret-key", "token", Long.MAX_VALUE));
  }

  private static Stream<Credential> staticCredentials() {
    return Stream.of(
        new JdbcCredential("user", "password"),
        new S3SecretKeyCredential("access-id", "secret-key"),
        new OSSSecretKeyCredential("access-id", "secret-key"),
        new AzureAccountKeyCredential("account", "key"),
        new AwsSecretKeyCredential("access-id", "secret-key"),
        new DlfSecretKeyCredential("access-id", "secret-key", "static-security-token"),
        new COSSecretKeyCredential("access-id", "secret-key"));
  }

  private void assertCredentialsForSecretPrivileges(
      List<Credential> credentials, List<Credential> tokens) {
    for (boolean useSecrets : new boolean[] {false, true}) {
      for (boolean includeCredentials : new boolean[] {false, true}) {
        try (MockedStatic<MetadataAuthzHelper> auth = mockStatic(MetadataAuthzHelper.class)) {
          mockSecretPrivileges(auth, useSecrets, includeCredentials);
          CatalogCredentialAuthzHandler handler = createHandler();
          try (Response original = credentialResponse(credentials);
              Response response = (Response) handler.processResponse(original)) {
            if (useSecrets && includeCredentials) {
              assertCredentials(response, credentials);
            } else if (!tokens.isEmpty() || credentials.isEmpty()) {
              assertCredentials(response, tokens);
            } else {
              Assertions.assertEquals(403, response.getStatus());
              Assertions.assertTrue(
                  ((ErrorResponse) response.getEntity())
                      .getMessage()
                      .contains(
                          "Catalog credentials require USE_SECRETS and INCLUDE_CREDENTIAL_SECRETS"));
            }
          }
        }
      }
    }
  }

  private CatalogCredentialAuthzHandler createHandler() {
    CatalogCredentialAuthzHandler handler = new CatalogCredentialAuthzHandler();
    handler.process(Map.of(Entity.EntityType.CATALOG, NameIdentifier.of("test", "catalog")));
    Assertions.assertFalse(handler.authorizationCompleted());
    Assertions.assertEquals(Entity.EntityType.CATALOG, handler.entityType());
    return handler;
  }

  private Response credentialResponse(List<Credential> credentials) {
    return Utils.ok(
        new CredentialResponse(DTOConverters.toDTO(credentials.toArray(new Credential[0]))));
  }

  private void mockSecretPrivileges(
      MockedStatic<MetadataAuthzHelper> auth, boolean useSecrets, boolean includeCredentials) {
    auth.when(
            () ->
                MetadataAuthzHelper.checkAccess(
                    any(),
                    any(Entity.EntityType.class),
                    eq(
                        AuthorizationExpressionConstants
                            .FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION)))
        .thenReturn(useSecrets);
    auth.when(
            () ->
                MetadataAuthzHelper.checkAccess(
                    any(),
                    any(Entity.EntityType.class),
                    eq(
                        AuthorizationExpressionConstants
                            .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION)))
        .thenReturn(includeCredentials);
  }

  private void assertCredentials(Response response, List<Credential> expected) {
    Assertions.assertEquals(200, response.getStatus());
    CredentialResponse result = (CredentialResponse) response.getEntity();
    Assertions.assertEquals(0, result.getCode());
    Assertions.assertEquals(expected.size(), result.getCredentials().length);
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertEquals(
          expected.get(i).toProperties(), result.getCredentials()[i].toProperties());
    }
  }
}
