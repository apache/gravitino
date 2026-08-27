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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.responses.SecretsResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
