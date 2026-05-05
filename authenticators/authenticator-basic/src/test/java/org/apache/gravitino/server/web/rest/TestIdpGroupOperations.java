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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.auth.local.IdpGroupManager;
import org.apache.gravitino.dto.IdpGroupDTO;
import org.apache.gravitino.dto.requests.CreateGroupRequest;
import org.apache.gravitino.dto.requests.UpdateGroupUsersRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.IdpGroupResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIdpGroupOperations {

  @Test
  public void testAddGroup() throws IllegalAccessException {
    CreateGroupRequest req = new CreateGroupRequest("group1");
    IdpGroupDTO group = buildGroup("group1");
    IdpGroupManager groupManager = mock(IdpGroupManager.class);
    IdpGroupOperations operations = buildOperations(groupManager);

    when(groupManager.createGroup("group1")).thenReturn(group);

    // test with IllegalRequest
    CreateGroupRequest illegalReq = new CreateGroupRequest("");
    Response illegalResp = operations.addGroup(illegalReq);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = (ErrorResponse) illegalResp.getEntity();
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp = operations.addGroup(req);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = (IdpGroupResponse) resp.getEntity();
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw GroupAlreadyExistsException
    doThrow(new GroupAlreadyExistsException("mock error")).when(groupManager).createGroup("group1");
    Response resp1 = operations.addGroup(req);

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        GroupAlreadyExistsException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    Mockito.reset(groupManager);
    when(groupManager.createGroup("group1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.addGroup(req);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testGetGroup() throws IllegalAccessException {
    IdpGroupDTO group = buildGroup("group1");
    IdpGroupManager groupManager = mock(IdpGroupManager.class);
    IdpGroupOperations operations = buildOperations(groupManager);

    when(groupManager.getGroup("group1")).thenReturn(group);

    Response resp = operations.getGroup("group1");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = (IdpGroupResponse) resp.getEntity();
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchGroupException
    doThrow(new NoSuchGroupException("mock error")).when(groupManager).getGroup("group1");
    Response resp1 = operations.getGroup("group1");

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchGroupException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    Mockito.reset(groupManager);
    when(groupManager.getGroup("group1")).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.getGroup("group1");

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testRemoveGroup() throws IllegalAccessException {
    IdpGroupManager groupManager = mock(IdpGroupManager.class);
    IdpGroupOperations operations = buildOperations(groupManager);

    when(groupManager.deleteGroup("group1", false)).thenReturn(true);

    Response resp = operations.removeGroup("group1", false);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    RemoveResponse removeResponse = (RemoveResponse) resp.getEntity();
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove group
    when(groupManager.deleteGroup("group1", false)).thenReturn(false);
    Response resp1 = operations.removeGroup("group1", false);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    RemoveResponse removeResponse1 = (RemoveResponse) resp1.getEntity();
    Assertions.assertEquals(0, removeResponse1.getCode());
    Assertions.assertFalse(removeResponse1.removed());

    // Test to throw internal RuntimeException
    Mockito.reset(groupManager);
    when(groupManager.deleteGroup("group1", false)).thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.removeGroup("group1", false);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testAddUsers() throws IllegalAccessException {
    UpdateGroupUsersRequest req = new UpdateGroupUsersRequest(Collections.singletonList("user1"));
    IdpGroupDTO group = buildGroup("group1");
    IdpGroupManager groupManager = mock(IdpGroupManager.class);
    IdpGroupOperations operations = buildOperations(groupManager);

    when(groupManager.addUsersToGroup("group1", Collections.singletonList("user1")))
        .thenReturn(group);

    // test with IllegalRequest
    UpdateGroupUsersRequest illegalReq = new UpdateGroupUsersRequest(Collections.emptyList());
    Response illegalResp = operations.addUsers("group1", illegalReq);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = (ErrorResponse) illegalResp.getEntity();
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp = operations.addUsers("group1", req);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = (IdpGroupResponse) resp.getEntity();
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(groupManager)
        .addUsersToGroup("group1", Collections.singletonList("user1"));
    Response resp1 = operations.addUsers("group1", req);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    Mockito.reset(groupManager);
    when(groupManager.addUsersToGroup("group1", Collections.singletonList("user1")))
        .thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.addUsers("group1", req);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testRemoveUsers() throws IllegalAccessException {
    UpdateGroupUsersRequest req = new UpdateGroupUsersRequest(Collections.singletonList("user1"));
    IdpGroupDTO group = buildGroup("group1");
    IdpGroupManager groupManager = mock(IdpGroupManager.class);
    IdpGroupOperations operations = buildOperations(groupManager);

    when(groupManager.removeUsersFromGroup("group1", Collections.singletonList("user1")))
        .thenReturn(group);

    // test with IllegalRequest
    UpdateGroupUsersRequest illegalReq = new UpdateGroupUsersRequest(Collections.emptyList());
    Response illegalResp = operations.removeUsers("group1", illegalReq);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, illegalResp.getMediaType());

    ErrorResponse illegalResponse = (ErrorResponse) illegalResp.getEntity();
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, illegalResponse.getCode());
    Assertions.assertEquals(
        IllegalArgumentException.class.getSimpleName(), illegalResponse.getType());

    Response resp = operations.removeUsers("group1", req);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    IdpGroupResponse groupResponse = (IdpGroupResponse) resp.getEntity();
    Assertions.assertEquals(0, groupResponse.getCode());
    Assertions.assertEquals("group1", groupResponse.getGroup().name());
    Assertions.assertNotNull(groupResponse.getGroup().users());
    Assertions.assertTrue(groupResponse.getGroup().users().isEmpty());

    // Test to throw NoSuchGroupException
    doThrow(new NoSuchGroupException("mock error"))
        .when(groupManager)
        .removeUsersFromGroup("group1", Collections.singletonList("user1"));
    Response resp1 = operations.removeUsers("group1", req);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = (ErrorResponse) resp1.getEntity();
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchGroupException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    Mockito.reset(groupManager);
    when(groupManager.removeUsersFromGroup("group1", Collections.singletonList("user1")))
        .thenThrow(new RuntimeException("mock error"));
    Response resp2 = operations.removeUsers("group1", req);

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp2.getMediaType());

    ErrorResponse errorResponse1 = (ErrorResponse) resp2.getEntity();
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  private IdpGroupDTO buildGroup(String group) {
    return IdpGroupDTO.builder().withName(group).build();
  }

  private IdpGroupOperations buildOperations(IdpGroupManager groupManager)
      throws IllegalAccessException {
    IdpGroupOperations operations = new IdpGroupOperations(groupManager);
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getAttribute(anyString())).thenReturn(null);
    FieldUtils.writeField(operations, "httpRequest", request, true);
    return operations;
  }
}
