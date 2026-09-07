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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.SecretProviderListResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.secret.SecretProviderInfo;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.server.web.mapper.WebApplicationExceptionMapper;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretsProviderOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  private final SecretProviderRegistry secretProviderRegistry = mock(SecretProviderRegistry.class);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(SecretsProviderOperations.class);
    resourceConfig.register(WebApplicationExceptionMapper.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(secretProviderRegistry).to(SecretProviderRegistry.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @Test
  public void testListSecretProvidersEmpty() {
    when(secretProviderRegistry.listProviders()).thenReturn(List.of());

    Response response =
        target("/secrets/providers").request().accept("application/vnd.gravitino.v1+json").get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SecretProviderListResponse body = response.readEntity(SecretProviderListResponse.class);
    Assertions.assertEquals(0, body.getCode());
    Assertions.assertEquals(0, body.getProviders().length);
  }

  @Test
  public void testListSecretProvidersOmitsUri() {
    when(secretProviderRegistry.listProviders())
        .thenReturn(List.of(new SecretProviderInfo("vault", "vault", "https://vault.example.com")));

    Response response =
        target("/secrets/providers").request().accept("application/vnd.gravitino.v1+json").get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    String json = response.readEntity(String.class);
    Assertions.assertFalse(json.contains("uri"));
    Assertions.assertFalse(json.contains("vault.example.com"));

    SecretProviderListResponse body =
        target("/secrets/providers")
            .request()
            .accept("application/vnd.gravitino.v1+json")
            .get()
            .readEntity(SecretProviderListResponse.class);
    Assertions.assertEquals(1, body.getProviders().length);
    Assertions.assertEquals("vault", body.getProviders()[0].getName());
    Assertions.assertEquals("vault", body.getProviders()[0].getType());
  }

  @Test
  public void testUnsupportedMethodReturnsErrorResponse() {
    Response response =
        target("/secrets/providers")
            .request()
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
    ErrorResponse error = response.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, error.getCode());
  }
}
