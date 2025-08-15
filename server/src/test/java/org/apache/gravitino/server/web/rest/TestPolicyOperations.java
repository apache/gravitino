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

import static org.apache.gravitino.dto.util.DTOConverters.toDTO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicySetRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private final PolicyManager policyManager = mock(PolicyManager.class);

  private final String metalake = "test_metalake";

  private final AuditInfo testAuditInfo1 =
      AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build();

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(PolicyOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(policyManager).to(PolicyDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListPolicies() {
    String[] policies = new String[] {"policy1", "policy2"};
    when(policyManager.listPolicies(metalake)).thenReturn(policies);

    Response response =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    NameListResponse nameListResponse = response.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    Assertions.assertArrayEquals(policies, nameListResponse.getNames());

    when(policyManager.listPolicies(metalake)).thenReturn(null);
    Response resp1 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    NameListResponse nameListResponse1 = resp1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());
    Assertions.assertEquals(0, nameListResponse1.getNames().length);

    when(policyManager.listPolicies(metalake)).thenReturn(new String[0]);
    Response resp2 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());

    NameListResponse nameListResponse2 = resp2.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse2.getCode());
    Assertions.assertEquals(0, nameListResponse2.getNames().length);

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(policyManager).listPolicies(metalake);
    Response resp3 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(policyManager).listPolicies(metalake);
    Response resp4 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp1 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testListPolicyInfos() {
    ImmutableMap<String, Object> contentFields = ImmutableMap.of("target_file_size_bytes", 1000);
    PolicyContent content =
        PolicyContents.custom(contentFields, ImmutableSet.of(MetadataObject.Type.TABLE), null);
    PolicyEntity policy1 =
        PolicyEntity.builder()
            .withId(1L)
            .withName("policy1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(content)
            .withAuditInfo(testAuditInfo1)
            .build();

    PolicyEntity policy2 =
        PolicyEntity.builder()
            .withId(1L)
            .withName("policy2")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(content)
            .withAuditInfo(testAuditInfo1)
            .build();

    PolicyEntity[] policies = new PolicyEntity[] {policy1, policy2};
    when(policyManager.listPolicyInfos(metalake)).thenReturn(policies);

    Response resp =
        target(policyPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PolicyListResponse policyListResp = resp.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResp.getCode());
    Assertions.assertEquals(policies.length, policyListResp.getPolicies().length);

    Assertions.assertEquals(policy1.name(), policyListResp.getPolicies()[0].name());
    Assertions.assertEquals(policy1.comment(), policyListResp.getPolicies()[0].comment());
    Assertions.assertEquals(Optional.empty(), policyListResp.getPolicies()[0].inherited());

    Assertions.assertEquals(policy2.name(), policyListResp.getPolicies()[1].name());
    Assertions.assertEquals(policy2.comment(), policyListResp.getPolicies()[1].comment());
    Assertions.assertEquals(Optional.empty(), policyListResp.getPolicies()[1].inherited());

    // Test return empty array
    when(policyManager.listPolicyInfos(metalake)).thenReturn(new PolicyEntity[0]);
    Response resp2 =
        target(policyPath(metalake))
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());

    PolicyListResponse policyListResp2 = resp2.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResp2.getCode());
    Assertions.assertEquals(0, policyListResp2.getPolicies().length);
  }

  @Test
  public void testCreatePolicy() {
    ImmutableMap<String, Object> contentFields = ImmutableMap.of("target_file_size_bytes", 1000);
    PolicyContent content =
        PolicyContents.custom(contentFields, ImmutableSet.of(MetadataObject.Type.TABLE), null);
    PolicyEntity policy1 =
        PolicyEntity.builder()
            .withId(1L)
            .withName("policy1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(content)
            .withAuditInfo(testAuditInfo1)
            .build();
    when(policyManager.createPolicy(
            metalake, "policy1", Policy.BuiltInType.CUSTOM, null, false, content))
        .thenReturn(policy1);

    PolicyCreateRequest request =
        new PolicyCreateRequest("policy1", "custom", null, false, toDTO(content));
    Response resp =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PolicyResponse policyResp = resp.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResp.getCode());

    Policy respPolicy = policyResp.getPolicy();
    Assertions.assertEquals(policy1.name(), respPolicy.name());
    Assertions.assertEquals(policy1.comment(), respPolicy.comment());
    Assertions.assertEquals(Optional.empty(), respPolicy.inherited());

    // Test throw PolicyAlreadyExistsException
    doThrow(new PolicyAlreadyExistsException("mock error"))
        .when(policyManager)
        .createPolicy(any(), any(), any(), any(), anyBoolean(), any());
    Response resp1 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp.getCode());
    Assertions.assertEquals(
        PolicyAlreadyExistsException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(policyManager)
        .createPolicy(any(), any(), any(), any(), anyBoolean(), any());

    Response resp2 =
        target(policyPath(metalake))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testGetPolicy() {
    ImmutableMap<String, Object> contentFields = ImmutableMap.of("target_file_size_bytes", 1000);
    PolicyContent content =
        PolicyContents.custom(contentFields, ImmutableSet.of(MetadataObject.Type.TABLE), null);
    PolicyEntity policy1 =
        PolicyEntity.builder()
            .withId(1L)
            .withName("policy1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withEnabled(false)
            .withContent(content)
            .withAuditInfo(testAuditInfo1)
            .build();
    when(policyManager.getPolicy(metalake, "policy1")).thenReturn(policy1);

    Response resp =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PolicyResponse policyResp = resp.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResp.getCode());

    Policy respPolicy = policyResp.getPolicy();
    Assertions.assertEquals(policy1.name(), respPolicy.name());
    Assertions.assertEquals(policy1.comment(), respPolicy.comment());
    Assertions.assertEquals(Optional.empty(), respPolicy.inherited());

    // Test throw NoSuchPolicyException
    doThrow(new NoSuchPolicyException("mock error"))
        .when(policyManager)
        .getPolicy(metalake, "policy1");

    Response resp2 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(policyManager).getPolicy(metalake, "policy1");

    Response resp3 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp1 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testAlterPolicy() {
    ImmutableMap<String, Object> contentFields = ImmutableMap.of("target_file_size_bytes", 1000);
    PolicyContent content =
        PolicyContents.custom(contentFields, ImmutableSet.of(MetadataObject.Type.TABLE), null);
    PolicyEntity newPolicy =
        PolicyEntity.builder()
            .withId(1L)
            .withName("new_policy1")
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withComment("new policy1 comment")
            .withEnabled(false)
            .withContent(content)
            .withAuditInfo(testAuditInfo1)
            .build();

    PolicyChange[] changes =
        new PolicyChange[] {
          PolicyChange.rename("new_policy1"),
          PolicyChange.updateComment("new policy1 comment"),
          PolicyChange.updateContent("custom", content)
        };

    when(policyManager.alterPolicy(metalake, "policy1", changes)).thenReturn(newPolicy);

    PolicyUpdateRequest[] requests =
        new PolicyUpdateRequest[] {
          new PolicyUpdateRequest.RenamePolicyRequest("new_policy1"),
          new PolicyUpdateRequest.UpdatePolicyCommentRequest("new policy1 comment"),
          new PolicyUpdateRequest.UpdatePolicyContentRequest("custom", toDTO(content))
        };
    PolicyUpdatesRequest request = new PolicyUpdatesRequest(Lists.newArrayList(requests));
    Response resp =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    PolicyResponse policyResp = resp.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResp.getCode());

    Policy respPolicy = policyResp.getPolicy();
    Assertions.assertEquals(newPolicy.name(), respPolicy.name());
    Assertions.assertEquals(newPolicy.comment(), respPolicy.comment());
    Assertions.assertEquals(Optional.empty(), respPolicy.inherited());

    // Test throw NoSuchPolicyException
    doThrow(new NoSuchPolicyException("mock error"))
        .when(policyManager)
        .alterPolicy(any(), any(), any());

    Response resp1 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(policyManager)
        .alterPolicy(any(), any(), any());

    Response resp2 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testSetPolicy() {
    PolicySetRequest req = new PolicySetRequest(true);
    doNothing().when(policyManager).enablePolicy(any(), any());

    Response resp =
        target(policyPath(metalake))
            .path("policy1")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BaseResponse baseResponse = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResponse.getCode());

    req = new PolicySetRequest(false);
    doNothing().when(policyManager).disablePolicy(any(), any());

    Response resp1 =
        target(policyPath(metalake))
            .path("policy1")
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .method("PATCH", Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    BaseResponse baseResponse1 = resp1.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResponse1.getCode());
  }

  @Test
  public void testDeletePolicy() {
    when(policyManager.deletePolicy(metalake, "policy1")).thenReturn(true);

    Response resp =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    when(policyManager.deletePolicy(metalake, "policy1")).thenReturn(false);
    Response resp1 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(policyManager).deletePolicy(any(), any());

    Response resp2 =
        target(policyPath(metalake))
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testListMetadataObjectForPolicy() {
    MetadataObject[] objects =
        new MetadataObject[] {
          MetadataObjects.parse("object1", MetadataObject.Type.CATALOG),
          MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA),
          MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE),
        };

    when(policyManager.listMetadataObjectsForPolicy(metalake, "policy1")).thenReturn(objects);

    Response response =
        target(policyPath(metalake))
            .path("policy1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    MetadataObjectListResponse objectListResponse =
        response.readEntity(MetadataObjectListResponse.class);
    Assertions.assertEquals(0, objectListResponse.getCode());

    MetadataObject[] respObjects = objectListResponse.getMetadataObjects();
    Assertions.assertEquals(objects.length, respObjects.length);

    for (int i = 0; i < objects.length; i++) {
      Assertions.assertEquals(objects[i].type(), respObjects[i].type());
      Assertions.assertEquals(objects[i].fullName(), respObjects[i].fullName());
    }

    // Test throw NoSuchPolicyException
    doThrow(new NoSuchPolicyException("mock error"))
        .when(policyManager)
        .listMetadataObjectsForPolicy(metalake, "policy1");

    Response response1 =
        target(policyPath(metalake))
            .path("policy1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response1.getStatus());

    ErrorResponse errorResponse = response1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResponse.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(policyManager)
        .listMetadataObjectsForPolicy(any(), any());

    Response response2 =
        target(policyPath(metalake))
            .path("policy1")
            .path("objects")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response2.getStatus());

    ErrorResponse errorResponse1 = response2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  private String policyPath(String metalake) {
    return "/metalakes/" + metalake + "/policies";
  }
}
