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

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
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
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetadataObjectPolicyOperations extends BaseOperationsTest {

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

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(false).when(config).get(CACHE_ENABLED);
    Mockito.doReturn(false).when(config).get(ENABLE_AUTHORIZATION);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
  }

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

    PolicyEntity[] schemaPolicyInfos =
        new PolicyEntity[] {createPolicy("policy1"), createPolicy("policy3")};
    when(policyManager.listPolicyInfosForMetadataObject(metalake, schema))
        .thenReturn(schemaPolicyInfos);

    PolicyEntity[] tablePolicyInfos = {
      createPolicy("policy1"), createPolicy("policy3"), createPolicy("policy5")
    };
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
    Assertions.assertTrue(resultPolicies.get("policy1").inherited().isEmpty());

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
    Assertions.assertEquals(schemaPolicyInfos.length, policyListResponse1.getPolicies().length);

    Map<String, Policy> resultPolicies1 =
        Arrays.stream(policyListResponse1.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies1.containsKey("policy1"));
    Assertions.assertTrue(resultPolicies1.containsKey("policy3"));

    Assertions.assertTrue(resultPolicies1.get("policy1").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies1.get("policy3").inherited().isEmpty());

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
    Assertions.assertEquals(schemaPolicyInfos.length, nameListResponse1.getNames().length);
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
    Assertions.assertEquals(tablePolicyInfos.length, policyListResponse2.getPolicies().length);

    Map<String, Policy> resultPolicies2 =
        Arrays.stream(policyListResponse2.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies2.containsKey("policy1"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy3"));
    Assertions.assertTrue(resultPolicies2.containsKey("policy5"));

    Assertions.assertTrue(resultPolicies2.get("policy1").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies2.get("policy3").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies2.get("policy5").inherited().isEmpty());

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
    Assertions.assertEquals(tablePolicyInfos.length, nameListResponse2.getNames().length);

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

    Assertions.assertTrue(resultPolicies8.get("policy1").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies8.get("policy3").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies8.get("policy5").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies8.get("policy0").inherited().isEmpty());
  }

  @Test
  public void testListPoliciesForObjectUnderHierarchicalSchema() {
    // The policy manager resolves policies from effective tags, including tags inherited from
    // hierarchical schemas and the catalog.
    MetadataObject table = MetadataObjects.parse("hcat.a:b:c.tbl", MetadataObject.Type.TABLE);

    when(policyManager.listPolicyInfosForMetadataObject(metalake, table))
        .thenReturn(
            new PolicyEntity[] {
              createPolicy("tablePolicy"),
              createPolicy("schemaCPolicy"),
              createPolicy("schemaBPolicy"),
              createPolicy("schemaAPolicy"),
              createPolicy("catalogPolicy")
            });

    Response response =
        target(basePath(metalake))
            .path(table.type().toString())
            .path(table.fullName())
            .path("policies")
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    PolicyListResponse policyListResponse = response.readEntity(PolicyListResponse.class);
    Assertions.assertEquals(0, policyListResponse.getCode());
    Assertions.assertEquals(5, policyListResponse.getPolicies().length);

    Map<String, Policy> resultPolicies =
        Arrays.stream(policyListResponse.getPolicies())
            .collect(Collectors.toMap(Policy::name, Function.identity()));

    Assertions.assertTrue(resultPolicies.get("tablePolicy").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies.get("schemaCPolicy").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies.get("schemaBPolicy").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies.get("schemaAPolicy").inherited().isEmpty());
    Assertions.assertTrue(resultPolicies.get("catalogPolicy").inherited().isEmpty());
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
