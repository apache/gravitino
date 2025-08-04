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
  private final PolicyContent policyContent = PolicyContents.custom(null, null);

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

    PolicyEntity policy1 =
        createPolicy(
            "policy1",
            "type1",
            true /* exclusive */,
            true /* inheritable */,
            ImmutableSet.of(MetadataObject.Type.CATALOG, MetadataObject.Type.TABLE));
    Policy[] catalogPolicyInfos = new Policy[] {policy1};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, catalog))
        .thenReturn(catalogPolicyInfos);

    PolicyEntity policy2 =
        createPolicy(
            "policy2",
            "type2",
            false /* exclusive */,
            true /* inheritable */,
            ImmutableSet.of(MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE));
    Policy[] schemaPolicyInfos = new Policy[] {policy2};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, schema))
        .thenReturn(schemaPolicyInfos);
    PolicyEntity policy3 =
        createPolicy(
            "policy3",
            policy1.policyType(),
            policy1.exclusive(),
            policy1.inheritable(),
            policy1.supportedObjectTypes());
    PolicyEntity policy4 =
        createPolicy(
            "policy4",
            policy2.policyType(),
            policy2.exclusive(),
            policy2.inheritable(),
            policy2.supportedObjectTypes());
    Policy[] tablePolicyInfos = new Policy[] {policy3, policy4};
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
        Arrays.stream(catalogPolicyInfos).map(Policy::name).toArray(String[]::new),
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
    Assertions.assertEquals(1, policyListResponse1.getPolicies().length);

    Map<String, Policy> resultPolicies1 =
        Arrays.stream(policyListResponse1.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies1.containsKey("policy2"));
    Assertions.assertFalse(resultPolicies1.get("policy2").inherited().get());

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
    Assertions.assertEquals(1, nameListResponse1.getNames().length);
    Set<String> resultNames = Sets.newHashSet(nameListResponse1.getNames());
    Assertions.assertTrue(resultNames.contains("policy2"));

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
    Assertions.assertEquals(3, policyListResponse2.getPolicies().length);

    Map<String, Policy> resultPolicies2 =
        Arrays.stream(policyListResponse2.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies2.containsKey("policy2"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy3"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy4"));

    Assertions.assertTrue(resultPolicies2.get("policy2").inherited().get());
    Assertions.assertFalse(resultPolicies2.get("policy3").inherited().get());
    Assertions.assertFalse(resultPolicies2.get("policy4").inherited().get());

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
    Assertions.assertEquals(3, nameListResponse2.getNames().length);

    Set<String> resultNames1 = Sets.newHashSet(nameListResponse2.getNames());
    Assertions.assertTrue(resultNames1.contains("policy2"));
    Assertions.assertTrue(resultNames1.contains("policy3"));
    Assertions.assertTrue(resultNames1.contains("policy4"));
  }

  @Test
  public void testGetPolicyForObject() {
    PolicyEntity policy1 =
        createPolicy(
            "policy1",
            "type1",
            true /* exclusive */,
            true /* inheritable */,
            Policy.SUPPORTS_ALL_OBJECT_TYPES);
    MetadataObject catalog = MetadataObjects.parse("object1", MetadataObject.Type.CATALOG);
    when(policyManager.getPolicyForMetadataObject(metalake, catalog, "policy1"))
        .thenReturn(policy1);
    when(policyManager.getPolicy(metalake, "policy1")).thenReturn(policy1);
    when(policyManager.listPolicyInfosForMetadataObject(metalake, catalog))
        .thenReturn(new Policy[] {policy1});

    PolicyEntity policy2 =
        createPolicy(
            "policy2",
            "type2",
            false /* exclusive */,
            true /* inheritable */,
            ImmutableSet.of(MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE));
    MetadataObject schema = MetadataObjects.parse("object1.object2", MetadataObject.Type.SCHEMA);
    when(policyManager.getPolicyForMetadataObject(metalake, schema, "policy2")).thenReturn(policy2);
    when(policyManager.getPolicy(metalake, "policy2")).thenReturn(policy2);
    when(policyManager.listPolicyInfosForMetadataObject(metalake, schema))
        .thenReturn(new Policy[] {policy2});

    PolicyEntity policy3 =
        createPolicy(
            "policy3",
            policy1.policyType(),
            policy1.exclusive(),
            policy1.inheritable(),
            policy1.supportedObjectTypes());
    MetadataObject table =
        MetadataObjects.parse("object1.object2.object3", MetadataObject.Type.TABLE);
    when(policyManager.getPolicyForMetadataObject(metalake, table, "policy3")).thenReturn(policy3);
    when(policyManager.getPolicy(metalake, "policy3")).thenReturn(policy3);
    when(policyManager.listPolicyInfosForMetadataObject(metalake, table))
        .thenReturn(new Policy[] {policy3});

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
    Assertions.assertFalse(respPolicy.inherited().get());
    Assertions.assertEquals(toDTO(policy1, respPolicy.inherited()), respPolicy);

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
    Assertions.assertFalse(respPolicy1.inherited().get());
    Assertions.assertEquals(toDTO(policy2, respPolicy1.inherited()), respPolicy1);

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
    Assertions.assertFalse(respPolicy2.inherited().get());
    Assertions.assertEquals(toDTO(policy3, respPolicy2.inherited()), respPolicy2);

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
    Assertions.assertTrue(respPolicy4.inherited().get());
    Assertions.assertEquals(toDTO(policy1, respPolicy4.inherited()), respPolicy4);

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
    Assertions.assertTrue(respPolicy5.inherited().get());
    Assertions.assertEquals(toDTO(policy2, respPolicy5.inherited()), respPolicy5);

    // Test table exclusive inherited policy not found
    Response response6 =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .path("policy1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response6.getStatus());

    ErrorResponse errorResponse = response6.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchPolicyException.class.getSimpleName(), errorResponse.getType());

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

    errorResponse = response7.readEntity(ErrorResponse.class);
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

  private PolicyEntity createPolicy(
      String policyName,
      String policyType,
      boolean exclusive,
      boolean inheritable,
      Set<MetadataObject.Type> supportedObjectTypes) {
    return PolicyEntity.builder()
        .withName(policyName)
        .withId(1L)
        .withPolicyType(policyType)
        .withExclusive(exclusive)
        .withInheritable(inheritable)
        .withSupportedObjectTypes(supportedObjectTypes)
        .withContent(policyContent)
        .withAuditInfo(testAuditInfo1)
        .build();
  }
}
