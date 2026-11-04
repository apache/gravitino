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
package org.apache.gravitino.lance.service;

import java.io.IOException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ServerHealth;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link LanceExceptionMapper}. */
public class TestLanceExceptionMapper extends JerseyTest {

  /** Verifies direct and wrapped OOM are recorded by the Lance error mapper. */
  @Test
  public void testMappedOutOfMemoryRecordsFailure() {
    for (Throwable failure :
        new Throwable[] {
          new OutOfMemoryError("Metaspace"),
          new IllegalStateException(new OutOfMemoryError("Java heap space"))
        }) {
      ServerHealth health = new ServerHealth();
      try (MockedStatic<ServerHealth> shared = Mockito.mockStatic(ServerHealth.class)) {
        shared.when(ServerHealth::getInstance).thenReturn(health);
        try (Response response = LanceExceptionMapper.toRESTResponse("test", failure)) {
          Assertions.assertEquals(500, response.getStatus());
        }
      }
      Assertions.assertTrue(health.hasOutOfMemoryError());
    }
  }

  /** Verifies ordinary mapped failures do not mark the JVM unhealthy. */
  @Test
  public void testOrdinaryMappedFailureDoesNotRecordOutOfMemory() {
    ServerHealth health = new ServerHealth();
    try (MockedStatic<ServerHealth> shared = Mockito.mockStatic(ServerHealth.class)) {
      shared.when(ServerHealth::getInstance).thenReturn(health);
      try (Response response =
          LanceExceptionMapper.toRESTResponse(
              "test", new IllegalStateException("ordinary failure"))) {
        Assertions.assertEquals(500, response.getStatus());
      }
    }
    Assertions.assertFalse(health.hasOutOfMemoryError());
  }

  /** A resource that raises an error outside the operation-level exception handlers. */
  @Path("error")
  public static class ErrorResource {

    /**
     * Raises an assertion error.
     *
     * @return never returns normally
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String fail() {
      AssertionError error = new AssertionError("assertion failure");
      error.initCause(new IllegalStateException("root cause"));
      throw error;
    }

    /**
     * Accepts JSON to exercise unsupported request media types.
     *
     * @param body the request body
     * @return the accepted body
     */
    @POST
    @Path("consume")
    @Consumes(MediaType.APPLICATION_JSON)
    public String consume(String body) {
      return body;
    }
  }

  /**
   * Configures the test resource and Lance exception mapper.
   *
   * @return the test application
   */
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig()
        .register(ErrorResource.class)
        .register(LanceExceptionMapper.class)
        .register(JacksonFeature.class);
  }

  @Test
  void testMethodNotAllowed() {
    try (Response response = target("error").request().put(Entity.json("{}"))) {
      Assertions.assertEquals(405, response.getStatus());
      Assertions.assertTrue(response.getAllowedMethods().contains("GET"));
      Assertions.assertEquals(13, response.readEntity(ErrorResponse.class).getCode());
    }
  }

  @Test
  void testUnsupportedMediaType() {
    try (Response response = target("error/consume").request().post(Entity.text("body"))) {
      Assertions.assertEquals(415, response.getStatus());
      Assertions.assertEquals(13, response.readEntity(ErrorResponse.class).getCode());
    }
  }

  @Test
  void testNotAcceptable() {
    try (Response response = target("error").request(MediaType.TEXT_PLAIN).get()) {
      Assertions.assertEquals(406, response.getStatus());
    }
  }

  /** Verifies that an uncaught error is converted to a Lance internal error response. */
  @Test
  public void testErrorResponse() {
    try (Response response = target("error").request(MediaType.APPLICATION_JSON_TYPE).get()) {
      Assertions.assertEquals(
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
      ErrorResponse entity = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals("assertion failure", entity.getError());
      Assertions.assertEquals("", entity.getInstance());
      Assertions.assertTrue(
          entity.getDetail().contains("java.lang.AssertionError: assertion failure"));
      Assertions.assertTrue(
          entity.getDetail().contains("Caused by: java.lang.IllegalStateException: root cause"));
    }
  }
}
