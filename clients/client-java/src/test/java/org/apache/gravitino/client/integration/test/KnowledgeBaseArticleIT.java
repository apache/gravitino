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
package org.apache.gravitino.client.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ArticleRevisionConflictException;
import org.apache.gravitino.exceptions.NoSuchKnowledgeBaseArticleException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.kb.ArticleBody;
import org.apache.gravitino.kb.ArticleCreate;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.kb.ArticleObjectIdentifier;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.junit.jupiter.api.Test;

/** Exercises Article editing and search through the Java client and server. */
public class KnowledgeBaseArticleIT extends BaseIT {
  @Test
  void editSearchAndDeleteArticles() {
    String metalake = "kb_it_" + UUID.randomUUID().toString().replace("-", "");
    var lake = client.createMetalake(metalake, "", Map.of());
    var articles = lake.articleOperations();
    try {
      var catalog = lake.createCatalog("models", Catalog.Type.MODEL, "model", "", Map.of());
      var schema = catalog.asSchemas().createSchema("sales", "", Map.of());
      var model =
          catalog
              .asModelCatalog()
              .registerModel(NameIdentifier.of(schema.name(), "forecast"), "", Map.of());
      var target =
          ArticleObjectIdentifier.of(
              MetadataObject.Type.MODEL, List.of(catalog.name(), schema.name()), model.name());
      var initialBody = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Original");
      var first =
          articles.createArticle(
              ArticleCreate.builder()
                  .withName("收入")
                  .withTitle("Alpha")
                  .withBody(initialBody)
                  .addObject(target)
                  .build());
      var snapshot = articles.loadArticle(first.name());
      var content = articles.getArticleContent(first.name());
      assertEquals(snapshot.revisionToken(), content.revisionToken());
      assertEquals(initialBody, content.body());

      var updatedBody = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Shared 100%");
      var edit =
          new KnowledgeBaseArticleChange[] {KnowledgeBaseArticleChange.updateBody(updatedBody)};
      var updated = articles.alterArticle(first.name(), edit, content.revisionToken());
      assertNotEquals(content.revisionToken(), updated.revisionToken());
      assertThrows(
          ArticleRevisionConflictException.class,
          () ->
              articles.alterArticle(
                  first.name(),
                  new KnowledgeBaseArticleChange[] {
                    KnowledgeBaseArticleChange.updateBody(initialBody)
                  },
                  content.revisionToken()));
      assertEquals(updatedBody, articles.getArticleContent(first.name()).body());

      var second =
          articles.createArticle(
              ArticleCreate.builder()
                  .withTitle("Beta")
                  .withBody(updatedBody)
                  .addObject(target)
                  .build());
      var filter = ArticleListFilter.builder().withQuery("SHARED 100%").withLimit(1);
      var page = articles.listArticles(filter.build());
      assertEquals(first.name(), page.articles().get(0).name());
      assertTrue(page.hasMore());
      var next = articles.listArticles(filter.withOffset(1).build());
      assertEquals(second.name(), next.articles().get(0).name());
      assertFalse(next.hasMore());

      assertEquals(List.of(target), articles.listAssetBindings(first.name()).objects());
      var bound = articles.listArticlesForObject(target, false, 1, 0);
      assertEquals(1, bound.articles().size());
      assertEquals(first.name(), bound.articles().get(0).name());
      assertTrue(bound.hasMore());
      var nextBound = articles.listArticlesForObject(target, false, 1, 1);
      assertEquals(1, nextBound.articles().size());
      assertEquals(second.name(), nextBound.articles().get(0).name());
      assertFalse(nextBound.hasMore());

      assertTrue(articles.deleteArticle(first.name()));
      assertFalse(articles.deleteArticle(first.name()));
      assertThrows(
          NoSuchKnowledgeBaseArticleException.class, () -> articles.loadArticle(first.name()));
    } finally {
      client.dropMetalake(metalake, true);
    }
  }
}
