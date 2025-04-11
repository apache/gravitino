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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
  public void testConnectionFailed() {
    Response response = Utils.connectionFailed("Connection failed");
    assertNotNull(response);
    assertEquals(Response.Status.BAD_GATEWAY.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Connection failed", errorResponse.getMessage());
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

  @Test
  public void testFilterFilesetAuditHeaders() {
    // test invalid internal client type
    HttpServletRequest mockRequest1 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest1.getHeader(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE))
        .thenReturn("test");
    Map<String, String> auditMap1 = Utils.filterFilesetAuditHeaders(mockRequest1);
    Assertions.assertEquals(
        InternalClientType.UNKNOWN.name(),
        auditMap1.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));

    // test invalid fileset data operation
    HttpServletRequest mockRequest2 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest2.getHeader(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION))
        .thenReturn("test");
    Map<String, String> auditMap2 = Utils.filterFilesetAuditHeaders(mockRequest2);
    Assertions.assertEquals(
        FilesetDataOperation.UNKNOWN.name(),
        auditMap2.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));

    // test normal audit headers
    HttpServletRequest mockRequest3 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest3.getHeader(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE))
        .thenReturn(InternalClientType.HADOOP_GVFS.name());
    when(mockRequest3.getHeader(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION))
        .thenReturn(FilesetDataOperation.GET_FILE_STATUS.name());
    Map<String, String> filteredMap = Utils.filterFilesetAuditHeaders(mockRequest3);
    Assertions.assertEquals(2, filteredMap.size());
    Assertions.assertEquals(
        InternalClientType.HADOOP_GVFS.name(),
        filteredMap.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));
    Assertions.assertEquals(
        FilesetDataOperation.GET_FILE_STATUS.name(),
        filteredMap.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));
  }
}
