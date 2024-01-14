/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.dto.responses.ErrorResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
public class TestUtils {

  @Test
  public void testRemoteUser() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn("user");
    String remoteUser = Utils.remoteUser(mockRequest);
    assertEquals("user", remoteUser);
  }

  @Test
  public void testRemoteUserDefault() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn(null);
    String remoteUser = Utils.remoteUser(mockRequest);
    assertEquals("gravitino", remoteUser);
  }

  @Test
  public void testOkWithData() {
    Response response = Utils.ok("data");
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals("data", response.getEntity());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
  }

  @Test
  public void testOkWithoutData() {
    Response response = Utils.ok();
    assertNotNull(response);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
  }

  @Test
  public void testIllegalArguments() {
    Response response = Utils.illegalArguments("Invalid argument");
    assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Invalid argument", errorResponse.getMessage());
  }

  @Test
  public void testInternalError() {
    Response response = Utils.internalError("Internal error");
    assertNotNull(response);
    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Internal error", errorResponse.getMessage());
  }

  @Test
  public void testNotFoundWithType() {
    Response response = Utils.notFound("Resource", "Not found");
    assertNotNull(response);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Resource", errorResponse.getType());
    assertEquals("Not found", errorResponse.getMessage());
  }

  @Test
  public void testNotFoundWithThrowable() {
    Throwable throwable = new RuntimeException("Some error");
    Response response = Utils.notFound("Resource", throwable);
    assertNotNull(response);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("RuntimeException", errorResponse.getType());
    assertEquals("Resource", errorResponse.getMessage());
  }

  @Test
  public void testAlreadyExistsWithType() {
    Response response = Utils.alreadyExists("Resource", "Already exists");
    assertNotNull(response);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Resource", errorResponse.getType());
    assertEquals("Already exists", errorResponse.getMessage());
  }

  @Test
  public void testAlreadyExistsWithThrowable() {
    Throwable throwable = new RuntimeException("Already exists");
    Response response = Utils.alreadyExists("New message", throwable);
    assertNotNull(response);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("RuntimeException", errorResponse.getType());
    assertEquals("New message", errorResponse.getMessage());
  }

  @Test
  public void testUnsupportedOperation() {
    Response response = Utils.unsupportedOperation("Unsupported operation");
    assertNotNull(response);
    assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Unsupported operation", errorResponse.getMessage());
  }
}
