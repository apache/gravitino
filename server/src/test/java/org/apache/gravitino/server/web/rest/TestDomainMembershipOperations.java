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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainMemberInfo;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainMemberListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

public class TestDomainMembershipOperations extends BaseOperationsTest {

  private static final String MEMBERS = "/metalakes/test/domains/Finance/objects";
  private static final String OBJECT_DOMAINS =
      "/metalakes/test/objects/table/cat.schema.table/domains";

  private final DomainDispatcher dispatcher = mock(DomainDispatcher.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  @Override
  protected void configureClient(ClientConfig config) {
    config.register(new ObjectMapperProvider());
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig(
            DomainMembershipOperations.class, MetadataObjectDomainOperations.class)
        .register(new ObjectMapperProvider())
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(dispatcher).to(DomainDispatcher.class);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @Test
  public void testMemberFiltersAndRelations() {
    MetadataObject table = MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE);
    DomainRelation[] relations = {
      DomainRelation.direct(),
      DomainRelation.inherited(MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA)),
      DomainRelation.viaSubdomain("Tax_Trading")
    };
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any()))
        .thenReturn(new DomainMember[] {new DomainMemberInfo(table, relations)});
    try (Response response =
        target(MEMBERS)
            .queryParam("types", "table,VIEW")
            .queryParam("domains", "Tax_Trading", "Markets_US")
            .queryParam("nameContains", "TaBlE")
            .request()
            .get()) {
      assertEquals(200, response.getStatus());
      DomainMemberListResponse result = response.readEntity(DomainMemberListResponse.class);
      result.validate();
      assertEquals(1, result.getMembers().length);
      assertEquals(table.fullName(), result.getMembers()[0].getObject().fullName());
      assertEquals(table.type(), result.getMembers()[0].getObject().type());
      assertEquals(3, result.getMembers()[0].getRelations().length);
      assertEquals(
          DomainRelation.Type.DIRECT, result.getMembers()[0].getRelations()[0].getRelation());
      assertNull(result.getMembers()[0].getRelations()[0].getSource());
      assertEquals("cat.schema", result.getMembers()[0].getRelations()[1].getSource().fullName());
      assertEquals("Tax_Trading", result.getMembers()[0].getRelations()[2].getSource().name());
    }
    ArgumentCaptor<DomainMemberFilter> filter = ArgumentCaptor.forClass(DomainMemberFilter.class);
    verify(dispatcher).listMembers(eq("test"), eq("Finance"), filter.capture());
    assertEquals(
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW)
            .withDomains("Tax_Trading", "Markets_US")
            .withNameContains("TaBlE")
            .build(),
        filter.getValue());
  }

  @Test
  public void testDefaultFiltersAndEmptyResults() {
    when(dispatcher.listMembers("test", "Finance", DomainMemberFilter.builder().build()))
        .thenReturn(new DomainMember[0]);
    try (Response response = target(MEMBERS).request().get()) {
      assertEquals(200, response.getStatus());
      assertEquals(0, response.readEntity(DomainMemberListResponse.class).getMembers().length);
    }
    when(dispatcher.listDomainsForMetadataObject(eq("test"), any()))
        .thenReturn(new DomainEntity[0]);
    try (Response response = target(OBJECT_DOMAINS).request().get()) {
      assertEquals(200, response.getStatus());
      assertEquals(0, response.readEntity(NameListResponse.class).getNames().length);
    }
  }

  @Test
  public void testDirectContainerMembersAndExpand() {
    MetadataObject catalog = MetadataObjects.parse("cat", MetadataObject.Type.CATALOG);
    MetadataObject schema = MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA);
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any()))
        .thenReturn(
            new DomainMember[] {
              new DomainMemberInfo(catalog, new DomainRelation[] {DomainRelation.direct()}),
              new DomainMemberInfo(
                  schema, new DomainRelation[] {DomainRelation.viaSubdomain("Tax_Trading")})
            });
    try (Response response =
        target(MEMBERS).queryParam("types", "catalog, SCHEMA").request().get()) {
      assertEquals(200, response.getStatus());
      DomainMemberListResponse result = response.readEntity(DomainMemberListResponse.class);
      result.validate();
      assertEquals(2, result.getMembers().length);
      assertEquals(MetadataObject.Type.CATALOG, result.getMembers()[0].getObject().type());
      assertEquals("cat", result.getMembers()[0].getObject().fullName());
      assertEquals(MetadataObject.Type.SCHEMA, result.getMembers()[1].getObject().type());
      assertEquals(
          DomainRelation.Type.VIA_SUBDOMAIN,
          result.getMembers()[1].getRelations()[0].getRelation());
    }
    try (Response response =
        target(MEMBERS).queryParam("types", "TABLE").queryParam("expand", true).request().get()) {
      assertEquals(200, response.getStatus());
    }
    ArgumentCaptor<DomainMemberFilter> filter = ArgumentCaptor.forClass(DomainMemberFilter.class);
    verify(dispatcher, times(2)).listMembers(eq("test"), eq("Finance"), filter.capture());
    assertEquals(
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA)
            .build(),
        filter.getAllValues().get(0));
    assertEquals(
        DomainMemberFilter.builder().withTypes(MetadataObject.Type.TABLE).withExpand(true).build(),
        filter.getAllValues().get(1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CATALOG", "SCHEMA", "TABLE,schema"})
  public void testContainerTypesRejectedWithExpand(String types) {
    try (Response response =
        target(MEMBERS).queryParam("types", types).queryParam("expand", "true").request().get()) {
      assertEquals(400, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals("IllegalArgumentException", error.getType());
      assertTrue(error.getMessage().contains("expand"));
    }
    verifyNoInteractions(dispatcher);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TABLE", "VIEW", "FILESET", "TOPIC", "MODEL", "SEMANTIC_MODEL"})
  public void testSupportedMemberTypes(String type) {
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any())).thenReturn(new DomainMember[0]);
    try (Response response = target(MEMBERS).queryParam("types", type).request().get()) {
      assertEquals(200, response.getStatus());
    }
    when(dispatcher.listDomainsForMetadataObject(eq("test"), any()))
        .thenReturn(new DomainEntity[0]);
    try (Response response =
        target("/metalakes/test/objects/" + type + "/cat.schema.member/domains")
            .queryParam("details", true)
            .request()
            .get()) {
      assertEquals(200, response.getStatus());
      assertEquals(0, response.readEntity(DomainListResponse.class).getDomains().length);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", "TABLE,", ",TABLE", "TABLE,,VIEW", "COLUMN", "FUNCTION", "DOMAIN", "UNKNOWN"})
  public void testInvalidTypes(String types) {
    try (Response response = target(MEMBERS).queryParam("types", types).request().get()) {
      assertEquals(400, response.getStatus());
      ErrorResponse error = response.readEntity(ErrorResponse.class);
      assertEquals("IllegalArgumentException", error.getType());
      assertTrue(error.getMessage().contains("Supported types:"));
      for (MetadataObject.Type supported : DomainMember.SUPPORTED_TYPES) {
        assertTrue(error.getMessage().contains(supported.name()));
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testInvalidDomainFilter() {
    try (Response response =
        target(MEMBERS).queryParam("domains", "Finance", "SyStEm").request().get()) {
      assertError(response, 400, "IllegalArgumentException");
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testBlankFilters() {
    for (String parameter : new String[] {"domains", "nameContains"}) {
      try (Response response = target(MEMBERS).queryParam(parameter, " ").request().get()) {
        assertError(response, 400, "IllegalArgumentException");
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @ParameterizedTest
  @ValueSource(strings = {"CATALOG", "SCHEMA", "COLUMN", "FUNCTION", "DOMAIN", "UNKNOWN"})
  public void testUnsupportedObjectTypes(String type) {
    try (Response response =
        target("/metalakes/test/objects/" + type + "/cat.schema.table/domains").request().get()) {
      assertError(response, 400, "IllegalArgumentException");
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testInvalidFullName() {
    try (Response response =
        target("/metalakes/test/objects/table/table/domains").request().get()) {
      assertError(response, 400, "IllegalArgumentException");
    }
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void testDomainSnapshotsAndNames() {
    DomainEntity domain =
        DomainEntity.builder()
            .withId(1L)
            .withName("Tax")
            .withNamespace(Namespace.of("test"))
            .withComment("Tax domain")
            .withProperties(Map.of("key", "value"))
            .withStatus(Domain.Status.DRAFT)
            .withParent("Finance")
            .withBackingTagId(2L)
            .withBackingTagName("department")
            .withBackingTagValue("tax")
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("user")
                    .withCreateTime(Instant.parse("2026-10-01T00:00:00Z"))
                    .build())
            .withRelations(new DomainRelation[] {DomainRelation.direct()})
            .build();
    when(dispatcher.listDomainsForMetadataObject(eq("test"), any()))
        .thenReturn(new DomainEntity[] {domain});
    try (Response response = target(OBJECT_DOMAINS).request().get()) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(
          new String[] {"Tax"}, response.readEntity(NameListResponse.class).getNames());
    }
    try (Response response = target(OBJECT_DOMAINS).queryParam("details", true).request().get()) {
      assertEquals(200, response.getStatus());
      DomainListResponse result = response.readEntity(DomainListResponse.class);
      result.validate();
      DomainDTO dto = result.getDomains()[0];
      assertEquals("Tax", dto.getName());
      assertEquals("Tax domain", dto.getComment());
      assertEquals(Map.of("key", "value"), dto.getProperties());
      assertEquals(Domain.Status.DRAFT, dto.getStatus());
      assertEquals("Finance", dto.getParent());
      assertEquals("department", dto.getBackingTag().getName());
      assertEquals("tax", dto.getBackingTag().getValue());
      assertEquals("user", dto.getAudit().creator());
      assertEquals(DomainRelation.Type.DIRECT, dto.getRelations()[0].getRelation());
    }
    verify(dispatcher, times(2))
        .listDomainsForMetadataObject(
            "test", MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE));
  }

  @Test
  public void testQueryErrors() {
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any()))
        .thenThrow(new NoSuchDomainException("Missing domain"));
    try (Response response = target(MEMBERS).request().get()) {
      assertError(response, 404, "NoSuchDomainException");
    }
    when(dispatcher.listDomainsForMetadataObject(eq("test"), any()))
        .thenThrow(new NoSuchMetalakeException("Missing metalake"));
    try (Response response = target(OBJECT_DOMAINS).request().get()) {
      assertError(response, 404, "NoSuchMetalakeException");
    }
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any()))
        .thenThrow(new RuntimeException("Store unavailable"));
    try (Response response = target(MEMBERS).request().get()) {
      assertError(response, 500, "RuntimeException");
    }
  }

  @Test
  public void testProductionMemberJsonRepresentation() throws IOException {
    DomainMember member =
        new DomainMemberInfo(
            MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE),
            new DomainRelation[] {
              DomainRelation.direct(),
              DomainRelation.inherited(
                  MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA)),
              DomainRelation.viaSubdomain("Finance_US")
            });
    when(dispatcher.listMembers(eq("test"), eq("Finance"), any()))
        .thenReturn(new DomainMember[] {member});
    try (Response response = target(MEMBERS).request().get()) {
      assertEquals(200, response.getStatus());
      JsonNode result =
          JsonUtils.objectMapper()
              .readTree(response.readEntity(String.class))
              .get("members")
              .get(0);
      assertEquals("table", result.get("object").get("type").asText());
      JsonNode relations = result.get("relations");
      assertEquals("direct", relations.get(0).get("relation").asText());
      assertFalse(relations.get(0).has("source"));
      assertEquals("inherited", relations.get(1).get("relation").asText());
      assertEquals("schema", relations.get(1).get("source").get("type").asText());
      assertEquals("via_subdomain", relations.get(2).get("relation").asText());
      assertEquals("domain", relations.get(2).get("source").get("type").asText());
      assertEquals("Finance_US", relations.get(2).get("source").get("fullName").asText());
    }
  }

  private static void assertError(Response response, int status, String type) {
    assertEquals(status, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    assertEquals(type, response.readEntity(ErrorResponse.class).getType());
  }
}
