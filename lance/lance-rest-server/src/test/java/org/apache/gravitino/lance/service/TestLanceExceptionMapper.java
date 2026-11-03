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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.model.ErrorResponse;

/** Tests for {@link LanceExceptionMapper}. */
public class TestLanceExceptionMapper extends JerseyTest {

  /** A resource that raises an error outside the operation-level exception handlers. */
  @Path("error")
  public static class ErrorResource {

    /**
     * Raises an assertion error.
     *
     * @return never returns normally
     */
    @GET
    public String fail() {
      AssertionError error = new AssertionError("assertion failure");
      error.initCause(new IllegalStateException("root cause"));
      throw error;
    }

    /**
     * Converts a framework response whose headers describe its original entity.
     *
     * @return the mapped Lance error response
     */
    @GET
    @Path("framework")
    public Response frameworkFailure() {
      try (Response original =
          Response.status(415)
              .header("content-encoding", "gzip")
              .header("content-length", "3")
              .header("Retry-After", "3")
              .entity("old")
              .build()) {
        return LanceExceptionMapper.toRESTResponse(
            "table", new WebApplicationException("request rejected", original));
      }
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

  /** Verifies that an uncaught error is converted to a Lance internal error response. */
  @Test
  public void testErrorResponse() {
    try (Response response = target("error").request(MediaType.APPLICATION_JSON_TYPE).get()) {
      Assertions.assertEquals(
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
      ErrorResponse entity = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals("Internal server error", entity.getError());
      Assertions.assertEquals("", entity.getInstance());
      Assertions.assertEquals("", entity.getDetail());
    }
  }

  /** Verifies backend authorization failures use the Lance forbidden response. */
  @Test
  public void testBackendForbidden() {
    assertAuthenticationError(new ForbiddenException("Access denied"), 403);
  }

  /** Verifies backend authentication failures use the Lance unauthenticated response. */
  @Test
  public void testBackendUnauthorized() {
    assertAuthenticationError(new UnauthorizedException("Invalid credentials"), 401);
  }

  /** Verifies unexpected exceptions do not expose internal details in the response. */
  @Test
  public void testInternalFailureDoesNotExposeException() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "catalog.schema.table", new RuntimeException("private-backend-detail"))) {
      Assertions.assertEquals(500, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals("Internal server error", error.getError());
      Assertions.assertEquals("", error.getDetail());
    }
  }

  /** Verifies intentional protocol validation details remain available to callers. */
  @Test
  public void testProtocolValidationDetailsArePreserved() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "table",
            new InvalidInputException("Invalid field", "field must be positive", "table"))) {
      Assertions.assertEquals(400, response.getStatus());
      Assertions.assertEquals(
          "field must be positive", ((ErrorResponse) response.getEntity()).getDetail());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 405, 406, 409, 415, 429})
  void testFrameworkClientStatusAndHeadersArePreserved(int status) {
    try (Response original =
            Response.status(status)
                .header("Allow", "GET")
                .header("WWW-Authenticate", "Basic realm=\"lance\"")
                .header("Retry-After", "3")
                .entity("framework entity")
                .build();
        Response response =
            LanceExceptionMapper.toRESTResponse(
                "table", new WebApplicationException("request rejected", original))) {
      Assertions.assertEquals(status, response.getStatus());
      Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals("request rejected", error.getError());
      Assertions.assertEquals("table", error.getInstance());
      Assertions.assertEquals("", error.getDetail());
      Assertions.assertNotEquals(new InternalException("", "", "").getCode(), error.getCode());
      Assertions.assertEquals("GET", response.getHeaderString("Allow"));
      Assertions.assertEquals(
          "Basic realm=\"lance\"", response.getHeaderString("WWW-Authenticate"));
      Assertions.assertEquals("3", response.getHeaderString("Retry-After"));
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Content-Length", "Content-Encoding", "Content-Language", "Content-Location",
        "Content-Range", "Content-Disposition", "Transfer-Encoding", "ETag",
        "Last-Modified", "Content-MD5", "Digest"
      })
  void testFrameworkEntityHeadersAreRemoved(String header) {
    try (Response original =
            Response.status(415).header(header, "original-entity-metadata").entity("old").build();
        Response response =
            LanceExceptionMapper.toRESTResponse(
                "table", new WebApplicationException("request rejected", original))) {
      Assertions.assertNull(response.getHeaderString(header));
      Assertions.assertEquals(415, response.getStatus());
      Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
      Assertions.assertEquals(
          "request rejected", ((ErrorResponse) response.getEntity()).getError());
    }
  }

  @Test
  void testMappedFrameworkEntityCanBeReadOverHttp() {
    try (Response response = target("error/framework").request().get()) {
      Assertions.assertEquals(415, response.getStatus());
      Assertions.assertNull(response.getHeaderString("Content-Encoding"));
      Assertions.assertEquals("3", response.getHeaderString("Retry-After"));
      Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals("request rejected", error.getError());
      Assertions.assertEquals("table", error.getInstance());
    }
  }

  @Test
  void testFrameworkServerErrorDoesNotExposeDetails() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "table", new WebApplicationException("private-backend-detail", 503))) {
      Assertions.assertEquals(503, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals("Internal server error", error.getError());
      Assertions.assertEquals("", error.getDetail());
    }
  }

  @Test
  void testUnknownRouteUsesLanceNotFoundEnvelope() {
    try (Response response = target("missing").request(MediaType.APPLICATION_JSON_TYPE).get()) {
      Assertions.assertEquals(404, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertNotNull(error.getCode());
      Assertions.assertEquals("", error.getDetail());
    }
  }

  private void assertAuthenticationError(Exception exception, int status) {
    try (Response response = LanceExceptionMapper.toRESTResponse("catalog", exception)) {
      Assertions.assertEquals(status, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals(exception.getMessage(), error.getError());
      Assertions.assertEquals("", error.getDetail());
      Assertions.assertEquals("catalog", error.getInstance());
    }
  }

  /** Verifies that the error detail omits the stack trace when disabled. */
  @Test
  public void testErrorDetailFollowsSetting() {
    IllegalArgumentException failure = new IllegalArgumentException("failure");
    try {
      Assertions.assertTrue(
          LanceExceptionMapper.errorDetail(failure)
              .contains("java.lang.IllegalArgumentException: failure"));

      LanceExceptionMapper.setIncludeErrorStackTrace(false);
      Assertions.assertEquals("", LanceExceptionMapper.errorDetail(failure));
      Response response = LanceExceptionMapper.toRESTResponse("", failure);
      Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      ErrorResponse entity = (ErrorResponse) response.getEntity();
      Assertions.assertEquals("failure", entity.getError());
      Assertions.assertEquals("", entity.getDetail());

      // Lance exceptions that already carry a detail keep it; only generated stacks are omitted.
      Response nativeResponse =
          LanceExceptionMapper.toRESTResponse(
              "instance", new InvalidInputException("bad input", "native detail", "instance"));
      Assertions.assertEquals(
          "native detail", ((ErrorResponse) nativeResponse.getEntity()).getDetail());
    } finally {
      LanceExceptionMapper.setIncludeErrorStackTrace(true);
    }
  }
}
