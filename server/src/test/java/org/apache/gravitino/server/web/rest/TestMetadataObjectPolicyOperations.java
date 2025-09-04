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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.PoliciesAssociateRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.policy.PolicyManager;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjectPolicyOperations extends JerseyTest {

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
  private final PolicyContent policyContent =
      PolicyContents.custom(null, ImmutableSet.of(MetadataObject.Type.TABLE), null);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(MetadataObjectPolicyOperations.class);
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
  public void testListPoliciesForObject() {
    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    MetadataObject schema = MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA);
    MetadataObject table =
        MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE);

    PolicyEntity[] catalogPolicyInfos = new PolicyEntity[] {createPolicy("policy1")};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, catalog))
        .thenReturn(catalogPolicyInfos);

    PolicyEntity[] schemaPolicyInfos = new PolicyEntity[] {createPolicy("policy3")};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, schema))
        .thenReturn(schemaPolicyInfos);

    PolicyEntity[] tablePolicyInfos = {createPolicy("policy5")};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, table))
        .thenReturn(tablePolicyInfos);

    // Test catalog policies
    Response response =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("/policies")
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    PolicyListResponse policyListResponse = response.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResponse.getCode());
    Assertions.assertEquals(catalogPolicyInfos.length, policyListResponse.getPolicies().length);

    Map<String, Policy> resultPolicies =
        Arrays.stream(policyListResponse.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies.containsKey("policy1"));
    Assertions.assertFalse(resultPolicies.get("policy1").inherited().get());

    Response response1 =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    NameListResponse nameListResponse = response1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    Assertions.assertEquals(catalogPolicyInfos.length, nameListResponse.getNames().length);
    Assertions.assertArrayEquals(
        Arrays.stream(catalogPolicyInfos).map(PolicyEntity::name).toArray(String[]::new),
        nameListResponse.getNames());

    // Test schema policies
    Response response2 =
        target(basePath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("policies")
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());

    PolicyListResponse policyListResponse1 = response2.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResponse1.getCode());
    Assertions.assertEquals(
        schemaPolicyInfos.length + catalogPolicyInfos.length,
        policyListResponse1.getPolicies().length);

    Map<String, Policy> resultPolicies1 =
        Arrays.stream(policyListResponse1.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies1.containsKey("policy1"));
    Assertions.assertTrue(resultPolicies1.containsKey("policy3"));

    Assertions.assertTrue(resultPolicies1.get("policy1").inherited().get());
    Assertions.assertFalse(resultPolicies1.get("policy3").inherited().get());

    Response response3 =
        target(basePath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response3.getStatus());

    NameListResponse nameListResponse1 = response3.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());
    Assertions.assertEquals(
        schemaPolicyInfos.length + catalogPolicyInfos.length, nameListResponse1.getNames().length);
    Set<String> resultNames = Sets.newHashSet(nameListResponse1.getNames());
    Assertions.assertTrue(resultNames.contains("policy1"));
    Assertions.assertTrue(resultNames.contains("policy3"));

    // Test table policies
    Response response4 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response4.getStatus());

    PolicyListResponse policyListResponse2 = response4.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResponse2.getCode());
    Assertions.assertEquals(
        schemaPolicyInfos.length + catalogPolicyInfos.length + tablePolicyInfos.length,
        policyListResponse2.getPolicies().length);

    Map<String, Policy> resultPolicies2 =
        Arrays.stream(policyListResponse2.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies2.containsKey("policy1"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy3"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy5"));

    Assertions.assertTrue(resultPolicies2.get("policy1").inherited().get());
    Assertions.assertTrue(resultPolicies2.get("policy3").inherited().get());
    Assertions.assertFalse(resultPolicies2.get("policy5").inherited().get());

    Response response5 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response5.getStatus());

    NameListResponse nameListResponse2 = response5.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse2.getCode());
    Assertions.assertEquals(
        schemaPolicyInfos.length + catalogPolicyInfos.length + tablePolicyInfos.length,
        nameListResponse2.getNames().length);

    Set<String> resultNames1 = Sets.newHashSet(nameListResponse2.getNames());
    Assertions.assertTrue(resultNames1.contains("policy1"));
    Assertions.assertTrue(resultNames1.contains("policy3"));
    Assertions.assertTrue(resultNames1.contains("policy5"));

    tablePolicyInfos =
        new PolicyEntity[] {
          createPolicy("policy5"),
          // Policy policy3 already associated with schema
          createPolicy("policy3"),
          // Policy policy1 already associated with catalog
          createPolicy("policy1"),
          createPolicy("policy0")
        };
    when(policyManager.listPolicyInfosForMetadataObject(metalake, table))
        .thenReturn(tablePolicyInfos);

    Response response8 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response8.getStatus());

    PolicyListResponse policyListResponse8 = response8.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResponse8.getCode());
    Assertions.assertEquals(4, policyListResponse8.getPolicies().length);

    Map<String, Policy> resultPolicies8 =
        Arrays.stream(policyListResponse8.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies8.containsKey("policy0"));
    Assertions.assertTrue(resultPolicies8.containsKey("policy1"));
    Assertions.assertTrue(resultPolicies8.containsKey("policy3"));
    Assertions.assertTrue(resultPolicies8.containsKey("policy5"));

    Assertions.assertFalse(resultPolicies8.get("policy1").inherited().get());
    Assertions.assertFalse(resultPolicies8.get("policy3").inherited().get());
    Assertions.assertFalse(resultPolicies8.get("policy5").inherited().get());
    Assertions.assertFalse(resultPolicies8.get("policy0").inherited().get());
  }

  @Test
  public void testGetPolicyForObject() {
    PolicyEntity policy1 = createPolicy("policy1");
    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    when(policyManager.getPolicyForMetadataObject(metalake, catalog, "policy1"))
        .thenReturn(policy1);

    PolicyEntity policy2 = createPolicy("policy2");
    MetadataObject schema = MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA);
    when(policyManager.getPolicyForMetadataObject(metalake, schema, "policy2")).thenReturn(policy2);

    PolicyEntity policy3 = createPolicy("policy3");
    MetadataObject table =
        MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE);
    when(policyManager.getPolicyForMetadataObject(metalake, table, "policy3")).thenReturn(policy3);

    // Test catalog policy
    Response response =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    PolicyResponse policyResponse = response.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResponse.getCode());

    Policy respPolicy = policyResponse.getPolicy();
    Assertions.assertEquals(policy1.name(), respPolicy.name());
    Assertions.assertEquals(policy1.comment(), respPolicy.comment());
    Assertions.assertFalse(respPolicy.inherited().get());

    // Test schema policy
    Response response1 =
        target(basePath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("policies")
            .path("policy2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    PolicyResponse policyResponse1 = response1.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResponse1.getCode());

    Policy respPolicy1 = policyResponse1.getPolicy();
    Assertions.assertEquals(policy2.name(), respPolicy1.name());
    Assertions.assertEquals(policy2.comment(), respPolicy1.comment());
    Assertions.assertFalse(respPolicy1.inherited().get());

    // Test table policy
    Response response2 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .path("policy3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response2.getStatus());

    PolicyResponse policyResponse2 = response2.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResponse2.getCode());

    Policy respPolicy2 = policyResponse2.getPolicy();
    Assertions.assertEquals(policy3.name(), respPolicy2.name());
    Assertions.assertEquals(policy3.comment(), respPolicy2.comment());
    Assertions.assertFalse(respPolicy2.inherited().get());

    // Test get schema inherited policy
    Response response4 =
        target(basePath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("policies")
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response4.getStatus());

    PolicyResponse policyResponse4 = response4.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResponse4.getCode());

    Policy respPolicy4 = policyResponse4.getPolicy();
    Assertions.assertEquals(policy1.name(), respPolicy4.name());
    Assertions.assertEquals(policy1.comment(), respPolicy4.comment());
    Assertions.assertTrue(respPolicy4.inherited().get());

    // Test get table inherited policy
    Response response5 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .path("policy2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response5.getStatus());

    PolicyResponse policyResponse5 = response5.readEntity(PolicyResponse.class);
    Assertions.assertEquals(0, policyResponse5.getCode());

    Policy respPolicy5 = policyResponse5.getPolicy();
    Assertions.assertEquals(policy2.name(), respPolicy5.name());
    Assertions.assertEquals(policy2.comment(), respPolicy5.comment());
    Assertions.assertTrue(respPolicy5.inherited().get());

    // Test catalog policy throw NoSuchPolicyException
    Response response7 =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .path("policy2")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response7.getStatus());

    ErrorResponse errorResponse = response7.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResponse.getType());

    // Test schema policy throw NoSuchPolicyException
    Response response8 =
        target(basePath(metalake))
            .path(schema.type().toString())
            .path(schema.fullName())
            .path("policies")
            .path("policy3")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response8.getStatus());

    ErrorResponse errorResponse1 = response8.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResponse1.getType());
  }

  @Test
  public void testAssociatePoliciesForObject() {
    String[] policiesToAdd = new String[] {"policy1", "policy2"};
    String[] policiesToRemove = new String[] {"policy3", "policy4"};

    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    when(policyManager.associatePoliciesForMetadataObject(
            metalake, catalog, policiesToAdd, policiesToRemove))
        .thenReturn(policiesToAdd);

    PoliciesAssociateRequest request =
        new PoliciesAssociateRequest(policiesToAdd, policiesToRemove);
    Response response =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    NameListResponse nameListResponse = response.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());

    Assertions.assertArrayEquals(policiesToAdd, nameListResponse.getNames());

    // Test throw null policies
    when(policyManager.associatePoliciesForMetadataObject(
            metalake, catalog, policiesToAdd, policiesToRemove))
        .thenReturn(null);
    Response response1 =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response1.getStatus());

    NameListResponse nameListResponse1 = response1.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse1.getCode());

    Assertions.assertEquals(0, nameListResponse1.getNames().length);

    // Test throw PolicyAlreadyAssociatedException
    doThrow(new PolicyAlreadyAssociatedException("mock error"))
        .when(policyManager)
        .associatePoliciesForMetadataObject(metalake, catalog, policiesToAdd, policiesToRemove);
    Response response2 =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), response2.getStatus());

    ErrorResponse errorResponse = response2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(
        PolicyAlreadyAssociatedException.class.getSimpleName(), errorResponse.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(policyManager)
        .associatePoliciesForMetadataObject(any(), any(), any(), any());

    Response response3 =
        target(basePath(metalake))
            .path(catalog.type().toString())
            .path(catalog.fullName())
            .path("policies")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response3.getStatus());

    ErrorResponse errorResponse1 = response3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse1.getType());
  }

  private String basePath(String metalake) {
    return "/metalakes/" + metalake + "/objects";
  }

  private PolicyEntity createPolicy(String policyName) {
    return PolicyEntity.builder()
        .withName(policyName)
        .withId(1L)
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withContent(policyContent)
        .withAuditInfo(testAuditInfo1)
        .build();
  }
}
