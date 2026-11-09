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

package org.apache.gravitino.dto.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainMemberListResponse;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainDTO {

  private static final String DOMAIN_JSON =
      "{\"name\":\"Finance\",\"comment\":\"Financial data\",\"properties\":{\"key\":\"value\"},"
          + "\"status\":\"published\",\"parent\":\"Parent\",\"backingTag\":{\"name\":\"department\",\"value\":\"finance\"},"
          + "\"audit\":{\"creator\":\"user\",\"createTime\":\"2026-10-01T00:00:00Z\",\"lastModifier\":null,\"lastModifiedTime\":null},"
          + "\"relations\":[{\"relation\":\"direct\"},{\"relation\":\"inherited\",\"source\":{\"type\":\"schema\",\"fullName\":\"cat.schema\"}},"
          + "{\"relation\":\"via_subdomain\",\"source\":{\"type\":\"domain\",\"fullName\":\"Finance.US\"}}]}";

  private static final String MEMBER_JSON =
      "{\"object\":{\"fullName\":\"cat.schema.table\",\"type\":\"table\"},"
          + "\"relations\":[{\"relation\":\"direct\"},{\"relation\":\"inherited\",\"source\":{\"type\":\"catalog\",\"fullName\":\"cat\"}},"
          + "{\"relation\":\"via_subdomain\",\"source\":{\"type\":\"domain\",\"fullName\":\"Finance.US\"}}]}";

  private static final AuditDTO AUDIT =
      AuditDTO.builder()
          .withCreator("user")
          .withCreateTime(Instant.parse("2026-10-01T00:00:00Z"))
          .build();

  @Test
  public void testDomainOptionalFieldsAndRelations() throws IOException {
    DomainRelation[] relations = {DomainRelation.direct(), DomainRelation.viaSubdomain("Tax/US")};
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    DomainDTO dto =
        new DomainDTO(
            "Finance", null, properties, Domain.Status.PUBLISHED, null, null, AUDIT, relations);
    properties.clear();
    relations[0] = null;
    DomainRelationDTO[] copy = dto.getRelations();
    copy[0] = null;
    DomainListResponse response = new DomainListResponse(new DomainDTO[] {dto});
    String json = JsonUtils.objectMapper().writeValueAsString(response);
    JsonNode tree = JsonUtils.objectMapper().readTree(json);
    JsonNode domain = tree.get("domains").get(0);
    Assertions.assertTrue(domain.get("backingTag").isNull());
    Assertions.assertTrue(domain.get("parent").isNull());
    Assertions.assertFalse(domain.get("relations").get(0).has("source"));
    DomainListResponse decoded = JsonUtils.objectMapper().readValue(json, DomainListResponse.class);
    decoded.validate();
    DomainDTO result = decoded.getDomains()[0];
    Assertions.assertEquals("value", result.getProperties().get("key"));
    Assertions.assertEquals(Domain.Status.PUBLISHED, result.getStatus());
    Assertions.assertEquals(DomainRelation.Type.DIRECT, result.getRelations()[0].getRelation());
    Assertions.assertEquals("Tax/US", result.getRelations()[1].getSource().fullName());
    Assertions.assertEquals(
        MetadataObject.Type.DOMAIN, result.getRelations()[1].getSource().type());
  }

  @Test
  public void testBackingTagValues() throws IOException {
    for (TagValue tag :
        new TagValue[] {TagValue.noValue("department"), TagValue.of("department", "finance")}) {
      DomainDTO dto =
          new DomainDTO(
              "Finance",
              "comment",
              Collections.emptyMap(),
              Domain.Status.DRAFT,
              "Parent",
              tag,
              AUDIT,
              new DomainRelation[] {DomainRelation.direct()});
      DomainDTO decoded =
          JsonUtils.objectMapper()
              .readValue(JsonUtils.objectMapper().writeValueAsString(dto), DomainDTO.class);
      Assertions.assertEquals(tag.name(), decoded.getBackingTag().getName());
      Assertions.assertEquals(tag.value().orElse(null), decoded.getBackingTag().getValue());
      Assertions.assertEquals("Parent", decoded.getParent());
      Assertions.assertEquals("comment", decoded.getComment());
      Assertions.assertEquals("user", decoded.getAudit().creator());
    }
  }

  @Test
  public void testMemberRoundTrip() throws IOException {
    DomainMember member =
        new DomainMember() {
          @Override
          public MetadataObject object() {
            return MetadataObjects.parse("cat.schema.table", MetadataObject.Type.TABLE);
          }

          @Override
          public DomainRelation[] relations() {
            return new DomainRelation[] {
              DomainRelation.inherited(
                  MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA))
            };
          }
        };
    DomainMemberDTO dto = DomainMemberDTO.from(member);
    DomainMemberDTO[] members = {dto};
    DomainMemberListResponse response = new DomainMemberListResponse(members);
    members[0] = null;
    response.getMembers()[0] = null;
    dto.getRelations()[0] = null;
    DomainMemberListResponse decoded =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(response),
                DomainMemberListResponse.class);
    decoded.validate();
    Assertions.assertEquals("cat.schema.table", decoded.getMembers()[0].getObject().fullName());
    Assertions.assertEquals(MetadataObject.Type.TABLE, decoded.getMembers()[0].getObject().type());
    Assertions.assertEquals(
        DomainRelation.Type.INHERITED, decoded.getMembers()[0].getRelations()[0].getRelation());
    Assertions.assertEquals(
        "cat.schema", decoded.getMembers()[0].getRelations()[0].getSource().fullName());
  }

  @Test
  public void testDottedSubdomainRelation() throws IOException {
    DomainRelation relation = DomainRelation.viaSubdomain("Finance.US");
    DomainRelationDTO decoded =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(DomainRelationDTO.from(relation)),
                DomainRelationDTO.class);
    Assertions.assertEquals(DomainRelation.Type.VIA_SUBDOMAIN, decoded.getRelation());
    Assertions.assertEquals(MetadataObject.Type.DOMAIN, decoded.getSource().type());
    Assertions.assertNull(decoded.getSource().parent());
    Assertions.assertEquals("Finance.US", decoded.getSource().name());
    Assertions.assertEquals(relation, DomainRelation.viaSubdomain(decoded.getSource().name()));
  }

  @Test
  public void testDomainJsonSerialization() throws IOException {
    DomainDTO dto =
        new DomainDTO(
            "Finance",
            "Financial data",
            Collections.singletonMap("key", "value"),
            Domain.Status.PUBLISHED,
            "Parent",
            TagValue.of("department", "finance"),
            AUDIT,
            new DomainRelation[] {
              DomainRelation.direct(),
              DomainRelation.inherited(
                  MetadataObjects.parse("cat.schema", MetadataObject.Type.SCHEMA)),
              DomainRelation.viaSubdomain("Finance.US")
            });
    assertJsonEquals(DOMAIN_JSON, dto);
  }

  @Test
  public void testDomainJsonDeserialization() throws IOException {
    DomainDTO dto = JsonUtils.objectMapper().readValue(DOMAIN_JSON, DomainDTO.class);
    Assertions.assertEquals("Finance", dto.getName());
    Assertions.assertEquals("Financial data", dto.getComment());
    Assertions.assertEquals(Collections.singletonMap("key", "value"), dto.getProperties());
    Assertions.assertEquals(Domain.Status.PUBLISHED, dto.getStatus());
    Assertions.assertEquals("Parent", dto.getParent());
    Assertions.assertEquals("department", dto.getBackingTag().getName());
    Assertions.assertEquals("finance", dto.getBackingTag().getValue());
    Assertions.assertEquals(AUDIT, dto.getAudit());
    Assertions.assertEquals(3, dto.getRelations().length);
    Assertions.assertEquals(DomainRelation.Type.DIRECT, dto.getRelations()[0].getRelation());
    Assertions.assertNull(dto.getRelations()[0].getSource());
    Assertions.assertEquals("cat.schema", dto.getRelations()[1].getSource().fullName());
    Assertions.assertEquals("Finance.US", dto.getRelations()[2].getSource().name());
    Assertions.assertNull(dto.getRelations()[2].getSource().parent());
    assertJsonEquals(DOMAIN_JSON, dto);
  }

  @Test
  public void testMemberJsonDeserializationAndSerialization() throws IOException {
    DomainMemberDTO member = JsonUtils.objectMapper().readValue(MEMBER_JSON, DomainMemberDTO.class);
    Assertions.assertEquals("cat.schema.table", member.getObject().fullName());
    Assertions.assertEquals(MetadataObject.Type.TABLE, member.getObject().type());
    Assertions.assertEquals(3, member.getRelations().length);
    Assertions.assertEquals(DomainRelation.Type.DIRECT, member.getRelations()[0].getRelation());
    Assertions.assertEquals(DomainRelation.Type.INHERITED, member.getRelations()[1].getRelation());
    Assertions.assertEquals("cat", member.getRelations()[1].getSource().name());
    Assertions.assertEquals(
        DomainRelation.Type.VIA_SUBDOMAIN, member.getRelations()[2].getRelation());
    Assertions.assertEquals("Finance.US", member.getRelations()[2].getSource().name());
    assertJsonEquals(MEMBER_JSON, member);
  }

  @Test
  public void testBackingTagJson() throws IOException {
    for (String json :
        new String[] {
          "{\"name\":\"department\",\"value\":\"finance\"}",
          "{\"name\":\"department\",\"value\":null}"
        }) {
      DomainBackingTagDTO tag = JsonUtils.objectMapper().readValue(json, DomainBackingTagDTO.class);
      Assertions.assertEquals("department", tag.getName());
      Assertions.assertEquals(
          JsonUtils.objectMapper().readTree(json).get("value").isNull() ? null : "finance",
          tag.getValue());
      tag.validate();
      assertJsonEquals(json, tag);
    }
    DomainBackingTagDTO noValue =
        JsonUtils.objectMapper().readValue("{\"name\":\"department\"}", DomainBackingTagDTO.class);
    Assertions.assertNull(noValue.getValue());
    assertJsonEquals("{\"name\":\"department\",\"value\":null}", noValue);
  }

  @Test
  public void testNullOrMissingRelations() throws IOException {
    for (String json : new String[] {"{}", "{\"relations\":null}", "{\"relations\":[]}"}) {
      DomainDTO domain = JsonUtils.objectMapper().readValue(json, DomainDTO.class);
      DomainMemberDTO member = JsonUtils.objectMapper().readValue(json, DomainMemberDTO.class);
      Assertions.assertEquals(0, domain.getRelations().length);
      Assertions.assertEquals(0, member.getRelations().length);
    }
  }

  private static void assertJsonEquals(String expected, Object value) throws IOException {
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected),
        JsonUtils.objectMapper().readTree(JsonUtils.objectMapper().writeValueAsString(value)));
  }
}
