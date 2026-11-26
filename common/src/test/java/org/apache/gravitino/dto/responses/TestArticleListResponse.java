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
package org.apache.gravitino.dto.responses;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.kb.ArticlePage;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.ArticleSummary;
import org.apache.gravitino.kb.ArticleTermBinding;
import org.junit.jupiter.api.Test;

class TestArticleListResponse {
  private final ObjectMapper mapper =
      JsonUtils.objectMapper().copy().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @Test
  void roundTripsSummaryFieldsAndContinuationFlags() throws IOException {
    Audit audit = AuditDTO.builder().withCreator("editor").withCreateTime(Instant.EPOCH).build();
    Owner owner = OwnerDTO.builder().withName("Finance").withType(Owner.Type.GROUP).build();
    ArticleSummary summary =
        new Summary(
            "guide",
            "Guide",
            ArticleStatus.DEPRECATED,
            new String[] {"Sales"},
            owner,
            new ArticleTermBinding[] {new Term("Revenue", ArticleTermBinding.Status.APPROVED)},
            true,
            audit);
    ArticleSummary unbound =
        new Summary(
            "other",
            "Other",
            ArticleStatus.PUBLISHED,
            new String[0],
            null,
            new ArticleTermBinding[0],
            false,
            audit);

    JsonNode expected =
        mapper.readTree(
            """
        {"code":0,"articles":[
          {"name":"guide","title":"Guide","status":"DEPRECATED","domains":["Sales"],
           "owner":{"name":"Finance","type":"group"},"terms":[{"name":"Revenue","status":"APPROVED"}],
           "termsHasMore":true,"audit_info":{"creator":"editor","createTime":"1970-01-01T00:00:00Z"}},
          {"name":"other","title":"Other","status":"PUBLISHED","domains":[],"owner":null,"terms":[],
           "termsHasMore":false,"audit_info":{"creator":"editor","createTime":"1970-01-01T00:00:00Z"}}],
         "hasMore":true}
        """);
    ArticleListResponse response =
        new ArticleListResponse(ArticlePage.of(List.of(summary, unbound), true));
    assertEquals(expected, mapper.valueToTree(response));
    assertEquals(
        expected, mapper.valueToTree(mapper.treeToValue(expected, ArticleListResponse.class)));
  }

  @Test
  void returnsAnEmptyArrayAtTheEndOfTheList() throws IOException {
    ArticleListResponse response = new ArticleListResponse(ArticlePage.of(List.of(), false));
    assertEquals(
        mapper.readTree("{\"code\":0,\"articles\":[],\"hasMore\":false}"),
        mapper.valueToTree(response));
  }

  record Summary(
      String name,
      String title,
      ArticleStatus status,
      String[] domains,
      @Nullable Owner owner,
      ArticleTermBinding[] terms,
      boolean termsHasMore,
      Audit auditInfo)
      implements ArticleSummary {}

  record Term(String name, ArticleTermBinding.Status status) implements ArticleTermBinding {}
}
