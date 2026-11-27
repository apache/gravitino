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
package org.apache.gravitino.dto.kb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.requests.ArticleCreateRequest;
import org.apache.gravitino.dto.requests.ArticleUpdatesRequest;
import org.apache.gravitino.dto.responses.ArticleResponse;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.apache.gravitino.kb.KnowledgeBaseArticle;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.junit.jupiter.api.Test;

class TestArticleClientConversions {

  private final ObjectMapper mapper =
      JsonUtils.objectMapper().copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private final ArticleBody body = ArticleBody.of(ArticleBody.Format.HTML, "<b>Net</b>");
  private final ArticleObjectIdentifier column =
      ArticleObjectIdentifier.of(
          MetadataObject.Type.COLUMN, List.of("catalog", "sales.ops", "orders"), "net.amount");

  @Test
  void serializesInitialFieldsAndBindings() throws IOException {
    ArticleCreate create =
        ArticleCreate.builder()
            .withName("net")
            .withTitle("Net")
            .withBody(body)
            .withStatus(ArticleStatus.PUBLISHED)
            .addDomain("Finance")
            .addTerms("Revenue")
            .addSynonym("Net sales")
            .withSourceReference("accounting")
            .withComment("reviewed")
            .withProperty("currency", "USD")
            .addObject(column)
            .build();
    JsonNode fields = mapper.valueToTree(ArticleCreateRequest.from(create));
    assertEquals(
        mapper.readTree(
            """
        {"name":"net","title":"Net","body":{"format":"HTML","content":"<b>Net</b>"},
         "status":"PUBLISHED","domains":["Finance"],"terms":["Revenue"],"synonyms":["Net sales"],
         "sourceReference":"accounting","comment":"reviewed","properties":{"currency":"USD"},
         "objects":[{"type":"COLUMN","namespace":["catalog","sales.ops","orders"],"name":"net.amount"}]}
        """),
        fields);
    ArticleCreate minimal = ArticleCreate.builder().withTitle("Net").withBody(body).build();
    assertFalse(mapper.valueToTree(ArticleCreateRequest.from(minimal)).has("status"));
  }

  @Test
  void encodesAnOrderedEditAndItsOptionalGuard() throws IOException {
    KnowledgeBaseArticleChange[] changes = {
      KnowledgeBaseArticleChange.updateTitle("Net sales"),
      KnowledgeBaseArticleChange.updateBody(body),
      KnowledgeBaseArticleChange.updateDomains(new String[] {"Finance"}),
      KnowledgeBaseArticleChange.updateStatus(ArticleStatus.PUBLISHED),
      KnowledgeBaseArticleChange.updateComment(null),
      KnowledgeBaseArticleChange.updateSynonyms(new String[] {"Net"}),
      KnowledgeBaseArticleChange.updateSourceReference(null),
      KnowledgeBaseArticleChange.setProperty("currency", "USD"),
      KnowledgeBaseArticleChange.removeProperty("currency"),
      KnowledgeBaseArticleChange.addTerms("Revenue"),
      KnowledgeBaseArticleChange.removeTerms("Revenue"),
      KnowledgeBaseArticleChange.addObjects(column),
      KnowledgeBaseArticleChange.removeObjects(column)
    };
    JsonNode request = mapper.valueToTree(ArticleUpdatesRequest.from(changes, "rev"));
    assertEquals("rev", request.get("expectedRevision").asText());
    assertTrue(request.at("/updates/4/newComment").isNull());
    assertTrue(request.at("/updates/6/newSourceReference").isNull());
    assertArrayEquals(changes, mapper.treeToValue(request, ArticleUpdatesRequest.class).changes());
    assertFalse(
        mapper.valueToTree(ArticleUpdatesRequest.from(changes, null)).has("expectedRevision"));
  }

  @Test
  void readsAnImmutableSnapshotWithVisibleBindingPreviews() throws IOException {
    KnowledgeBaseArticle article =
        mapper
            .readValue(
                """
        {"code":0,"article":{"name":"net","title":"Net sales","status":"PUBLISHED",
         "domains":["Finance"],"synonyms":["Net"],"properties":{"currency":"USD"},"owner":null,
         "objects":[{"type":"COLUMN","namespace":["catalog","sales.ops","orders"],"name":"net.amount"}],
         "objectsHasMore":true,"terms":[{"name":"Revenue","status":"APPROVED"}],"termsHasMore":false,
         "revisionToken":"rev","audit_info":{"creator":"editor","createTime":"1970-01-01T00:00:00Z"}}}
        """,
                ArticleResponse.class)
            .getArticle();
    article.domains()[0] = "Other";
    article.terms()[0] = null;
    article.objects()[0] = null;
    assertThrows(UnsupportedOperationException.class, () -> article.properties().clear());
    assertArrayEquals(new String[] {"Finance"}, article.domains());
    assertEquals(ArticleTermBinding.Status.APPROVED, article.terms()[0].status());
    assertEquals("Revenue", article.terms()[0].name());
    assertArrayEquals(new ArticleObjectIdentifier[] {column}, article.objects());
    assertTrue(article.objectsHasMore());
    assertFalse(article.termsHasMore());
    assertNull(article.owner());
    assertEquals("rev", article.revisionToken());
    assertEquals("editor", article.auditInfo().creator());
  }
}
