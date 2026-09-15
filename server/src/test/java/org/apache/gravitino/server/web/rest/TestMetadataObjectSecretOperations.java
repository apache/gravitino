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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.responses.SecretsResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestMetadataObjectSecretOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private SecretPropertyOperationDispatcher secretPropertyOperationDispatcher =
      mock(SecretPropertyOperationDispatcher.class);

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
    resourceConfig.register(MetadataObjectSecretOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(secretPropertyOperationDispatcher)
                .to(SecretPropertyOperationDispatcher.class)
                .ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testGetSecretsForCatalog() {
    testGetSecretsForObject(MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetSecretsForSchema() {
    testGetSecretsForObject(MetadataObjects.parse("catalog.schema", MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testGetSecretsForFileset() {
    testGetSecretsForObject(
        MetadataObjects.parse("catalog.schema.fileset", MetadataObject.Type.FILESET));
  }

  @Test
  public void testGetSecretsForMetalake() {
    testGetSecretsForObject(MetadataObjects.parse("test_metalake", MetadataObject.Type.METALAKE));
  }

  @Test
  public void testGetSecretsForTopic() {
    testGetSecretsForObject(
        MetadataObjects.parse("catalog.schema.topic", MetadataObject.Type.TOPIC));
  }

  @Test
  public void testGetSecretsForView() {
    testGetSecretsForObject(MetadataObjects.parse("catalog.schema.view", MetadataObject.Type.VIEW));
  }

  @Test
  public void testGetSecretsForModel() {
    testGetSecretsForObject(
        MetadataObjects.parse("catalog.schema.model", MetadataObject.Type.MODEL));
  }

  @Test
  public void testGetSecretsForModelVersion() {
    testGetSecretsForObject(
        MetadataObjects.parse("catalog.schema.model.0", MetadataObject.Type.MODEL_VERSION));
  }

  @Test
  public void testGetSecretsReturnsEmptyWithoutUseSecret() throws Exception {
    MetadataObject metadataObject =
        MetadataObjects.parse("catalog.schema.fileset", MetadataObject.Type.FILESET);
    when(secretPropertyOperationDispatcher.getSecrets(any(), any(Entity.EntityType.class)))
        .thenReturn(Map.of("custom-secret", "plaintext"));

    MetadataObjectSecretOperations operations =
        new MetadataObjectSecretOperations(secretPropertyOperationDispatcher);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);

    try (MockedStatic<MetadataAuthzHelper> metadataAuthzHelper =
        mockStatic(MetadataAuthzHelper.class)) {
      metadataAuthzHelper
          .when(
              () ->
                  MetadataAuthzHelper.checkAccess(
                      any(),
                      any(Entity.EntityType.class),
                      eq(
                          AuthorizationExpressionConstants
                              .FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION)))
          .thenReturn(false);

      Response response =
          operations.getSecrets(metalake, metadataObject.type().name(), metadataObject.fullName());

      Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      SecretsResponse secretResponse = (SecretsResponse) response.getEntity();
      Assertions.assertEquals(0, secretResponse.getCode());
      Assertions.assertTrue(secretResponse.getSecrets().isEmpty());
      verify(secretPropertyOperationDispatcher, never())
          .getSecrets(any(), any(Entity.EntityType.class));
    }
  }

  private void testGetSecretsForObject(MetadataObject metadataObject) {
    when(secretPropertyOperationDispatcher.getSecrets(any(), any(Entity.EntityType.class)))
        .thenReturn(Map.of("custom-secret", "plaintext"));

    Response response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/secrets")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SecretsResponse secretResponse = response.readEntity(SecretsResponse.class);
    Assertions.assertEquals(0, secretResponse.getCode());
    Assertions.assertEquals("plaintext", secretResponse.getSecrets().get("custom-secret"));

    when(secretPropertyOperationDispatcher.getSecrets(any(), any(Entity.EntityType.class)))
        .thenReturn(Map.of());
    response =
        target(basePath(metalake))
            .path(metadataObject.type().toString())
            .path(metadataObject.fullName())
            .path("/secrets")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    secretResponse = response.readEntity(SecretsResponse.class);
    Assertions.assertEquals(0, secretResponse.getCode());
    Assertions.assertTrue(secretResponse.getSecrets().isEmpty());
  }

  private String basePath(String metalake) {
    return "/metalakes/" + metalake + "/objects";
  }
}
