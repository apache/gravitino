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

import com.fasterxml.jackson.core.JsonParseException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ReaderInterceptorContext;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.ErrorFactory;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.errors.ThrottlingException;
import org.lance.namespace.errors.UnsupportedOperationException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.ErrorResponse;
import org.mockito.Mockito;

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
    /**
     * Requires a numeric path value so Jersey exercises path conversion failures.
     *
     * @param version the version parsed by Jersey
     * @return the parsed version
     */
    @GET
    @Path("path/{version}")
    public String version(@PathParam("version") int version) {
      return String.valueOf(version);
    }

    /**
     * Reads a JSON request using the same Jackson reader configuration as the service.
     *
     * @param request the decoded namespace request
     * @return its mode
     */
    @POST
    @Path("json")
    @Consumes(MediaType.APPLICATION_JSON)
    public String json(CreateNamespaceRequest request) {
      return request.getMode();
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
        .register(JacksonFeature.withoutExceptionMappers())
        .register(LanceRequestBodyReaderInterceptor.class);
  }

  @Test
  void testMethodNotAllowed() {
    try (Response response = target("error").request().put(Entity.json("{}"))) {
      Assertions.assertEquals(405, response.getStatus());
      Assertions.assertTrue(response.getAllowedMethods().contains("GET"));
      Assertions.assertEquals(
          ErrorCode.INVALID_INPUT.getCode(), response.readEntity(ErrorResponse.class).getCode());
    }
  }

  @Test
  void testUnsupportedMediaType() {
    try (Response response = target("error/consume").request().post(Entity.text("body"))) {
      Assertions.assertEquals(415, response.getStatus());
      Assertions.assertEquals(
          ErrorCode.INVALID_INPUT.getCode(), response.readEntity(ErrorResponse.class).getCode());
    }
  }

  @Test
  void testNotAcceptable() {
    try (Response response = target("error").request(MediaType.TEXT_PLAIN).get()) {
      Assertions.assertEquals(406, response.getStatus());
      Assertions.assertEquals(
          ErrorCode.INVALID_INPUT.getCode(), response.readEntity(ErrorResponse.class).getCode());
    }
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
  @ValueSource(ints = {400, 401, 403, 405, 406, 409, 415, 429})
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "missing",
        "v1/table/catalog$schema$table/missing",
        "v1/namespace/catalog$schema/missing"
      })
  void testUnknownRouteUsesUnsupportedEnvelope(String path) {
    try (Response response = target(path).request(MediaType.APPLICATION_JSON_TYPE).get()) {
      Assertions.assertEquals(406, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals(ErrorCode.UNSUPPORTED.getCode(), error.getCode());
      Assertions.assertEquals("GET " + path, error.getInstance());
      Assertions.assertTrue(error.getError().contains(error.getInstance()));
      Assertions.assertEquals("", error.getDetail());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"query", "create_index"})
  void testUnimplementedOperationsAreClassifiedAsUnsupported(String operation) {
    String path = "v1/table/catalog$schema$table/" + operation;
    try (Response response = target(path).request().post(Entity.json("{}"))) {
      Assertions.assertEquals(406, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals(ErrorCode.UNSUPPORTED.getCode(), error.getCode());
      Assertions.assertEquals("POST " + path, error.getInstance());
      Assertions.assertTrue(error.getError().contains(error.getInstance()));
      Assertions.assertInstanceOf(
          UnsupportedOperationException.class,
          ErrorFactory.fromErrorCode(
              error.getCode(), error.getError(), error.getDetail(), error.getInstance()));
    }
  }

  @Test
  void testFramework404IsUnsupportedAndPreservesHeaders() {
    try (Response original = Response.status(404).header("Retry-After", "3").build();
        Response response =
            LanceExceptionMapper.toRESTResponse(
                "POST v1/table/table/query", new WebApplicationException(original))) {
      Assertions.assertEquals(406, response.getStatus());
      Assertions.assertEquals("3", response.getHeaderString("Retry-After"));
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals(ErrorCode.UNSUPPORTED.getCode(), error.getCode());
      Assertions.assertTrue(error.getError().contains("POST v1/table/table/query"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testValidationErrorsNeverIncludeStackTraces(boolean includeStackTrace) {
    IllegalArgumentException failure =
        new IllegalArgumentException(
            "Invalid column name", new IllegalStateException("private-backend-detail"));
    try {
      LanceExceptionMapper.setIncludeErrorStackTrace(includeStackTrace);
      try (Response response = LanceExceptionMapper.toRESTResponse("table", failure)) {
        Assertions.assertEquals(400, response.getStatus());
        ErrorResponse error = (ErrorResponse) response.getEntity();
        Assertions.assertEquals(ErrorCode.INVALID_INPUT.getCode(), error.getCode());
        Assertions.assertEquals("Invalid column name", error.getError());
        Assertions.assertEquals("", error.getDetail());
      }
    } finally {
      LanceExceptionMapper.setIncludeErrorStackTrace(true);
    }
  }

  @Test
  void testLogLevelsSeparateClientErrorsFromServerFailures() {
    IllegalStateException internal = new IllegalStateException("private-backend-detail");
    WebApplicationException framework = new WebApplicationException("framework failure", 503);
    List<LogEvent> events =
        captureMapperLogs(
            () -> {
              for (Exception failure :
                  new Exception[] {
                    new IllegalArgumentException("Invalid column name"),
                    new NoSuchTableException("Table missing"),
                    internal,
                    framework
                  }) {
                try (Response ignored = LanceExceptionMapper.toRESTResponse("table", failure)) {
                  // Collect logs from the same mapper entry point used by operation handlers.
                }
              }
            });
    Assertions.assertEquals(
        List.of(Level.INFO, Level.INFO, Level.ERROR, Level.ERROR),
        events.stream().map(LogEvent::getLevel).toList());
    Assertions.assertNull(events.get(0).getThrown());
    Assertions.assertNull(events.get(1).getThrown());
    Assertions.assertSame(internal, events.get(2).getThrown());
    Assertions.assertSame(framework, events.get(3).getThrown());
  }

  @Test
  void testInvalidPathParameterIsInvalidInput() {
    try (Response response = target("error/path/not-a-number").request().get()) {
      Assertions.assertEquals(400, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals(ErrorCode.INVALID_INPUT.getCode(), error.getCode());
      Assertions.assertTrue(error.getError().contains("version"));
    }
  }

  @Test
  void testTypedEntityNotFoundErrorsKeepTheirCodes() {
    for (Exception exception :
        new Exception[] {
          new TableNotFoundException("table absent", "", "table"),
          new NamespaceNotFoundException("namespace absent", "", "namespace")
        }) {
      try (Response response = LanceExceptionMapper.toRESTResponse("instance", exception)) {
        Assertions.assertEquals(404, response.getStatus());
        ErrorResponse error = (ErrorResponse) response.getEntity();
        Assertions.assertEquals(
            exception instanceof TableNotFoundException
                ? ErrorCode.TABLE_NOT_FOUND.getCode()
                : ErrorCode.NAMESPACE_NOT_FOUND.getCode(),
            error.getCode());
      }
    }
  }

  @Test
  void testBackendThrottlingReturns429() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "table", new ThrottlingException("Slow down", "retry later", "table"))) {
      Assertions.assertEquals(429, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals(ErrorCode.THROTTLING.getCode(), error.getCode());
      Assertions.assertEquals("retry later", error.getDetail());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"{", "[]", "{\"mode\":{\"secret\":\"private-value\"}}"})
  void testInvalidJsonUsesLanceEnvelope(String json) {
    try (Response response = target("error/json").request().post(Entity.json(json))) {
      Assertions.assertEquals(400, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals(ErrorCode.INVALID_INPUT.getCode(), error.getCode());
      Assertions.assertEquals("Invalid JSON request", error.getError());
      Assertions.assertEquals("", error.getDetail());
    }
  }

  @Test
  void testValidJsonStillReachesResource() {
    try (Response response =
        target("error/json").request().post(Entity.json("{\"mode\":\"create\"}"))) {
      Assertions.assertEquals(200, response.getStatus());
      Assertions.assertEquals("create", response.readEntity(String.class));
    }
  }

  @Test
  void testReaderPreservesCauseAndDoesNotTranslateOtherFailures() throws IOException {
    ReaderInterceptorContext context = Mockito.mock(ReaderInterceptorContext.class);
    JsonParseException invalid = new JsonParseException(null, "private-value");
    Mockito.when(context.proceed()).thenThrow(invalid);
    LanceRequestBodyReaderInterceptor reader = new LanceRequestBodyReaderInterceptor();
    BadRequestException response =
        Assertions.assertThrows(BadRequestException.class, () -> reader.aroundReadFrom(context));
    Assertions.assertSame(invalid, response.getCause());
    Assertions.assertEquals("Invalid JSON request", response.getMessage());
    IOException readFailure = new IOException("stream failed");
    Mockito.doThrow(readFailure).when(context).proceed();
    Assertions.assertSame(
        readFailure,
        Assertions.assertThrows(IOException.class, () -> reader.aroundReadFrom(context)));
    try (Response backend = LanceExceptionMapper.toRESTResponse("table", invalid)) {
      Assertions.assertEquals(500, backend.getStatus());
      Assertions.assertEquals(
          "Internal server error", ((ErrorResponse) backend.getEntity()).getError());
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

  private static List<LogEvent> captureMapperLogs(Runnable action) {
    String name = LanceExceptionMapper.class.getName();
    LoggerContext context =
        (LoggerContext) LogManager.getContext(LanceExceptionMapper.class.getClassLoader(), false);
    AbstractConfiguration configuration = (AbstractConfiguration) context.getConfiguration();
    LoggerConfig previous = configuration.getLoggers().get(name);
    List<LogEvent> events = new CopyOnWriteArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("lanceMapperCapture", null, null, true, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    try {
      appender.start();
      configuration.addAppender(appender);
      LoggerConfig logger = new LoggerConfig(name, Level.ALL, false);
      logger.addAppender(appender, Level.ALL, null);
      configuration.addLogger(name, logger);
      context.updateLoggers();
      action.run();
      return events;
    } finally {
      configuration.removeLogger(name);
      if (previous != null) {
        configuration.addLogger(name, previous);
      }
      configuration.removeAppender(appender.getName());
      appender.stop();
      context.updateLoggers();
    }
  }
}
