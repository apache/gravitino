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
package org.apache.gravitino.dto.requests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.kb.ArticleDTO;
import org.apache.gravitino.dto.kb.ArticleObjectDTO;
import org.apache.gravitino.dto.kb.ArticleTermDTO;
import org.apache.gravitino.dto.responses.ArticleContentResponse;
import org.apache.gravitino.dto.responses.ArticleResponse;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleContent;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.junit.jupiter.api.Test;

class TestArticleRequests {
  private static final String BODY = "{\"format\":\"MARKDOWN\",\"content\":\"# 正文\"}";
  private static final String COLUMN =
      "{\"type\":\"COLUMN\",\"namespace\":[\"c\",\"s\",\"t\"],\"name\":\"total.amount\"}";
  private final ObjectMapper mapper =
      JsonUtils.objectMapper().copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final ArticleObjectIdentifier column =
      ArticleObjectIdentifier.of(
          MetadataObject.Type.COLUMN, List.of("c", "s", "t"), "total.amount");

  @Test
  void createConvertsFieldsAndPreservesIdentifierComponents() throws Exception {
    String json =
        """
        {"name":"指南","title":"Guide","status":"DRAFT","domains":["Sales"],"terms":["Cost"],
         "synonyms":["Manual"],"sourceReference":"ref","comment":"note","properties":{"k":"v"},
         "body":%s,"objects":[%s]}
        """
            .formatted(BODY, COLUMN);
    ArticleCreateRequest request = mapper.readValue(json, ArticleCreateRequest.class);
    ArticleCreate spec = request.toCreate();
    assertEquals(mapper.readTree(json), mapper.valueToTree(request));
    assertEquals("指南", spec.name());
    assertEquals("Guide", spec.title());
    assertEquals(ArticleBody.of(ArticleBody.Format.MARKDOWN, "# 正文"), spec.body());
    assertEquals(ArticleStatus.DRAFT, spec.status());
    assertEquals(List.of("Sales"), spec.domains());
    assertEquals(List.of("Cost"), spec.terms());
    assertEquals(List.of("Manual"), spec.synonyms());
    assertEquals("ref", spec.sourceReference());
    assertEquals("note", spec.comment());
    assertEquals(Map.of("k", "v"), spec.properties());
    assertEquals(List.of(column), spec.objects());

    ArticleCreate minimal =
        mapper
            .readValue("{\"title\":\"Guide\",\"body\":" + BODY + "}", ArticleCreateRequest.class)
            .toCreate();
    assertNull(minimal.name());
    assertNull(minimal.status());
    assertTrue(minimal.domains().isEmpty());
    assertTrue(minimal.synonyms().isEmpty());
    assertTrue(minimal.terms().isEmpty());
    assertTrue(minimal.objects().isEmpty());
    assertTrue(minimal.properties().isEmpty());
  }

  @Test
  void changesKeepTheirOrderAndExplicitNulls() throws Exception {
    String json =
        """
        {"expectedRevision":"token","updates":[
          {"@type":"updateTitle","newTitle":"Guide"},
          {"@type":"updateBody","newBody":%s},
          {"@type":"updateDomains","newDomains":["Sales"]},
          {"@type":"updateStatus","newStatus":"DRAFT"},
          {"@type":"updateComment","newComment":null},
          {"@type":"updateSynonyms","newSynonyms":[]},
          {"@type":"updateSourceReference","newSourceReference":null},
          {"@type":"setProperty","property":"key","value":"value"},
          {"@type":"removeProperty","property":"old"},
          {"@type":"addTerms","terms":["Cost"]},
          {"@type":"removeTerms","terms":["Revenue"]},
          {"@type":"addObjects","objects":[%s]},
          {"@type":"removeObjects","objects":[%s]}
        ]}
        """
            .formatted(BODY, COLUMN, COLUMN);
    ArticleUpdatesRequest request = mapper.readValue(json, ArticleUpdatesRequest.class);
    assertEquals(mapper.readTree(json), mapper.valueToTree(request));
    assertEquals("token", request.getExpectedRevision());
    assertArrayEquals(
        new KnowledgeBaseArticleChange[] {
          KnowledgeBaseArticleChange.updateTitle("Guide"),
          KnowledgeBaseArticleChange.updateBody(
              ArticleBody.of(ArticleBody.Format.MARKDOWN, "# 正文")),
          KnowledgeBaseArticleChange.updateDomains(new String[] {"Sales"}),
          KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DRAFT),
          KnowledgeBaseArticleChange.updateComment(null),
          KnowledgeBaseArticleChange.updateSynonyms(new String[0]),
          KnowledgeBaseArticleChange.updateSourceReference(null),
          KnowledgeBaseArticleChange.setProperty("key", "value"),
          KnowledgeBaseArticleChange.removeProperty("old"),
          KnowledgeBaseArticleChange.addTerms("Cost"),
          KnowledgeBaseArticleChange.removeTerms("Revenue"),
          KnowledgeBaseArticleChange.addObjects(column),
          KnowledgeBaseArticleChange.removeObjects(column)
        },
        request.changes());
  }

  @Test
  void rejectsInvalidChangesAndIdentifiers() throws Exception {
    for (String change :
        List.of(
            "{\"@type\":\"rename\",\"newName\":\"x\"}",
            "{\"@type\":\"updateComment\"}",
            "{\"@type\":\"updateSourceReference\"}",
            "{\"@type\":\"updateStatus\",\"newStatus\":null}",
            "{\"@type\":\"updateDomains\",\"newDomains\":[null]}",
            "{\"@type\":\"addObjects\",\"objects\":[null]}",
            "null")) {
      assertThrows(
          JsonProcessingException.class,
          () -> mapper.readValue("{\"updates\":[" + change + "]}", ArticleUpdatesRequest.class),
          change);
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> mapper.readValue("{\"updates\":[]}", ArticleUpdatesRequest.class).changes());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            mapper
                .readValue(
                    "{\"title\":\"Guide\",\"body\":"
                        + BODY
                        + ",\"objects\":[{\"type\":\"COLUMN\",\"namespace\":[\"c.s.t\"],\"name\":\"a\"}]}",
                    ArticleCreateRequest.class)
                .toCreate());
    for (String extra :
        List.of("\"terms\":[null]", "\"properties\":{\"k\":null}", "\"status\":null")) {
      assertThrows(
          JsonProcessingException.class,
          () ->
              mapper.readValue(
                  "{\"title\":\"Guide\",\"body\":" + BODY + "," + extra + "}",
                  ArticleCreateRequest.class));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> mapper.readValue("{\"title\":\"Guide\"}", ArticleCreateRequest.class).toCreate());
  }

  @Test
  void rejectsNonStringJsonValuesBeforeCreatingOrEditingAnArticle() {
    for (String json :
        List.of(
            "{\"title\":42,\"body\":" + BODY + "}",
            "{\"title\":\"Guide\",\"body\":{\"format\":1,\"content\":\"text\"}}",
            "{\"title\":\"Guide\",\"body\":{\"format\":\"MARKDOWN\",\"content\":true}}",
            "{\"title\":\"Guide\",\"body\":" + BODY + ",\"properties\":{\"k\":42}}",
            "{\"title\":\"Guide\",\"body\":" + BODY + ",\"terms\":[true]}",
            "{\"title\":\"Guide\",\"body\":"
                + BODY
                + ",\"objects\":[{\"type\":\"SCHEMA\",\"namespace\":[123],\"name\":\"s\"}]}")) {
      assertThrows(
          JsonProcessingException.class,
          () -> mapper.readValue(json, ArticleCreateRequest.class),
          json);
    }
    for (String json :
        List.of(
            "{\"expectedRevision\":true,\"updates\":[]}",
            "{\"updates\":[{\"@type\":\"updateComment\",\"newComment\":false}]}",
            "{\"updates\":[{\"@type\":\"addTerms\",\"terms\":[42]}]}",
            "{\"updates\":[{\"@type\":\"setProperty\",\"property\":\"k\",\"value\":3}]}")) {
      assertThrows(
          JsonProcessingException.class,
          () -> mapper.readValue(json, ArticleUpdatesRequest.class),
          json);
    }
  }

  @Test
  void metadataAndContentHaveDistinctShapesAndAnEditingToken() throws Exception {
    AuditDTO audit = AuditDTO.builder().withCreator("editor").withCreateTime(Instant.EPOCH).build();
    ArticleDTO article =
        new ArticleDTO(
            "guide",
            "Guide",
            ArticleStatus.PUBLISHED,
            List.of(),
            null,
            List.of(),
            null,
            Map.of(),
            List.of(new ArticleTermDTO("term", ArticleTermBinding.Status.APPROVED)),
            true,
            List.of(ArticleObjectDTO.from(column)),
            false,
            null,
            "token",
            audit);
    JsonNode metadata = mapper.valueToTree(new ArticleResponse(article));
    JsonNode fields = metadata.get("article");
    assertEquals(0, metadata.get("code").intValue());
    assertEquals("token", fields.get("revisionToken").textValue());
    assertEquals("PUBLISHED", fields.get("status").textValue());
    assertEquals("APPROVED", fields.at("/terms/0/status").textValue());
    assertEquals(mapper.readTree(COLUMN), fields.at("/objects/0"));
    assertTrue(fields.get("termsHasMore").booleanValue());
    assertFalse(fields.get("objectsHasMore").booleanValue());
    assertEquals("editor", fields.at("/audit_info/creator").textValue());
    assertTrue(fields.get("owner").isNull());
    assertFalse(fields.has("body"));
    assertEquals(metadata, mapper.valueToTree(mapper.treeToValue(metadata, ArticleResponse.class)));

    ArticleContent content =
        new ArticleContent(ArticleBody.of(ArticleBody.Format.MARKDOWN, "# 正文"), "token");
    JsonNode response = mapper.valueToTree(new ArticleContentResponse(content));
    assertEquals(
        mapper.readTree("{\"code\":0,\"body\":" + BODY + ",\"revisionToken\":\"token\"}"),
        response);
    assertEquals(
        response, mapper.valueToTree(mapper.treeToValue(response, ArticleContentResponse.class)));
  }
}
