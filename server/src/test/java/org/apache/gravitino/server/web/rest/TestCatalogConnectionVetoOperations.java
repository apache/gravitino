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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.CatalogEventDispatcher;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.TestConnectionPreEvent;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests HTTP responses and delegate isolation when a connection-test pre-listener vetoes a request.
 */
public class TestCatalogConnectionVetoOperations extends BaseOperationsTest {
  private static final String CATALOGS_PATH = "/metalakes/metalake1/catalogs/";
  private static final String DENIED_MESSAGE = "Connection test denied by pre-listener";

  private final CatalogDispatcher delegate = mock(CatalogDispatcher.class);
  private final EventListenerPlugin listener = mock(EventListenerPlugin.class);

  /** {@inheritDoc} */
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig(CatalogOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          /** {@inheritDoc} */
          @Override
          protected void configure() {
            CatalogEventDispatcher dispatcher =
                new CatalogEventDispatcher(
                    new EventBus(Collections.singletonList(listener)), delegate);
            bind(dispatcher).to(CatalogDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @Test
  void testPreCreateConnectionVeto() {
    CatalogCreateRequest request =
        new CatalogCreateRequest(
            "catalog1", Catalog.Type.RELATIONAL, "test", "comment", Collections.emptyMap());
    assertPreEventVeto("testConnection", Entity.json(request), true);
  }

  @Test
  void testStoredConnectionVeto() {
    assertPreEventVeto("catalog1/testConnection", null, false);
  }

  @Test
  void testProposedChangesConnectionVeto() {
    CatalogUpdatesRequest request =
        new CatalogUpdatesRequest(
            Collections.singletonList(
                new CatalogUpdateRequest.SetCatalogPropertyRequest("key", "value")));
    assertPreEventVeto("catalog1/testConnection", Entity.json(request), false);
  }

  @Test
  void testEmptyChangesConnectionVeto() {
    assertPreEventVeto(
        "catalog1/testConnection",
        Entity.json(new CatalogUpdatesRequest(Collections.emptyList())),
        false);
  }

  private void assertPreEventVeto(String path, @Nullable Entity<?> request, boolean includeStack) {
    doThrow(new ForbiddenException(DENIED_MESSAGE))
        .when(listener)
        .onPreEvent(any(TestConnectionPreEvent.class));

    try (Response response =
        target(CATALOGS_PATH + path)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(request)) {
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals(
          Response.Status.FORBIDDEN.getStatusCode(), response.getStatus(), error.getMessage());
      Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, error.getCode());
      Assertions.assertEquals(ForbiddenException.class.getSimpleName(), error.getType());
      Assertions.assertEquals(DENIED_MESSAGE, error.getMessage());
      if (includeStack) {
        Assertions.assertNotNull(error.getStack());
      } else {
        Assertions.assertNull(error.getStack());
      }
    }

    verify(listener).onPreEvent(any(TestConnectionPreEvent.class));
    verify(listener, never()).onPostEvent(any(Event.class));
    verifyNoInteractions(delegate);
  }

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    /** {@inheritDoc} */
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }
}
