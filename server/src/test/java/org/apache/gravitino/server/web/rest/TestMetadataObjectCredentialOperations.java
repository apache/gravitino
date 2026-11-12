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
package org.apache.gravitino.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialOperationDispatcher;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.dto.responses.CredentialResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestMetadataObjectCredentialOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private CredentialOperationDispatcher credentialOperationDispatcher =
      mock(CredentialOperationDispatcher.class);

  private String metalake = "test_metalake";

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(MetadataObjectCredentialOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(credentialOperationDispatcher).to(CredentialOperationDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  /** Checks catalog secret privileges without changing fileset authorization. */
  @Test
  public void testCatalogCredentialsRequireBothSecretPrivileges() throws Exception {
    for (boolean useSecrets : new boolean[] {false, true}) {
      for (boolean includeCredentials : new boolean[] {false, true}) {
        CredentialOperationDispatcher dispatcher = mock(CredentialOperationDispatcher.class);
        MetadataObjectCredentialOperations operations =
            new MetadataObjectCredentialOperations(dispatcher);
        FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
        try (MockedStatic<MetadataAuthzHelper> auth = mockStatic(MetadataAuthzHelper.class)) {
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
          try (Response response = operations.getCredentials("test", "catalog", "catalog")) {
            Assertions.assertEquals(
                useSecrets && includeCredentials ? 200 : 403, response.getStatus());
          }
          if (!useSecrets || !includeCredentials) verifyNoInteractions(dispatcher);
          try (Response response =
              operations.getCredentials("test", "fileset", "catalog.schema.fileset")) {
            Assertions.assertEquals(200, response.getStatus());
          }
        }
      }
    }
  }

  /** Checks credential client errors identify the requested object. */
  @Test
  public void testUnsupportedObjectAndProviderErrorsIdentifyTheObject() {
    Response unsupported =
        target(basePath(metalake))
            .path("table")
            .path("catalog.schema.table")
            .path("credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(400, unsupported.getStatus());
    String message = unsupported.readEntity(ErrorResponse.class).getMessage();
    Assertions.assertTrue(message.contains("catalog.schema.table"));
    Assertions.assertTrue(message.contains("TABLE"));
    when(credentialOperationDispatcher.getCredentials(any(), any()))
        .thenThrow(new IllegalArgumentException("Invalid provider"));
    Response invalid =
        target(basePath(metalake))
            .path("fileset")
            .path("catalog.schema.fileset")
            .path("credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(400, invalid.getStatus());
    Assertions.assertTrue(
        invalid.readEntity(ErrorResponse.class).getMessage().contains("catalog.schema.fileset"));
  }

  @Test
  public void testGetCredentialsForCatalog() {
    testGetCredentialsForObject(MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetCredentialsForFileset() {
    testGetCredentialsForObject(
        MetadataObjects.parse("catalog.schema.fileset", MetadataObject.Type.FILESET));
  }

  @Test
  public void testInvalidCredentialLocationResponse() {
    String path = "wasbs://container@account.blob.core.windows.net/warehouse/table";
    when(credentialOperationDispatcher.getCredentials(any(), any()))
        .thenThrow(
            new IllegalArgumentException(
                "adls-token requires an abfs or abfss location; unsupported location: " + path));

    Response response =
        target(basePath(metalake))
            .path("fileset")
            .path("catalog.schema.fileset")
            .path("credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    ErrorResponse error = response.readEntity(ErrorResponse.class);
    Assertions.assertTrue(error.getMessage().contains("abfs or abfss"));
    Assertions.assertTrue(error.getMessage().contains(path));
  }

  private void testGetCredentialsForObject(MetadataObject metadataObject) {

    S3SecretKeyCredential credential = new S3SecretKeyCredential("access-id", "secret-key");
    // Test return one credential
    when(credentialOperationDispatcher.getCredentials(any(), any()))
        .thenReturn(Arrays.asList(credential));
    Response response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    CredentialResponse credentialResponse = response.readEntity(CredentialResponse.class);
    Assertions.assertEquals(0, credentialResponse.getCode());
    Assertions.assertEquals(1, credentialResponse.getCredentials().length);
    Credential credentialToTest = DTOConverters.fromDTO(credentialResponse.getCredentials()[0]);
    Assertions.assertTrue(credentialToTest instanceof S3SecretKeyCredential);
    Assertions.assertEquals("access-id", ((S3SecretKeyCredential) credentialToTest).accessKeyId());
    Assertions.assertEquals(
        "secret-key", ((S3SecretKeyCredential) credentialToTest).secretAccessKey());
    Assertions.assertEquals(0, credentialToTest.expireTimeInMs());

    // Test doesn't return credential
    when(credentialOperationDispatcher.getCredentials(any(), any())).thenReturn(null);
    response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/credentials")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    credentialResponse = response.readEntity(CredentialResponse.class);
    Assertions.assertEquals(0, credentialResponse.getCode());
    Assertions.assertEquals(0, credentialResponse.getCredentials().length);
  }

  private String basePath(String metalake) {
    return "/metalakes/" + metalake + "/objects";
  }
}
