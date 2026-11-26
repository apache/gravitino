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

package org.apache.gravitino.kb;

import static org.apache.gravitino.storage.relational.service.GlossaryDomainFixtures.insertDomain;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.UnapprovedTermException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

class TestArticleAssetBindings extends TestJDBCBackend {
  private static final String LAKE = "lake";
  private static final Namespace NS = Namespace.of(LAKE, "cat", "sales");
  private final AtomicLong ids = new AtomicLong(100);
  private final ArticleManager articles = new ArticleManager(ids::incrementAndGet, 1024);

  @TestTemplate
  void nineTypesResolveAndRoundTripInBoundedPages() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "cat");
    createAndInsertSchema(LAKE, "cat", "sales");
    backend.insert(table(10, "orders", 17), false);
    backend.insert(createViewEntity(11L, NS, "asset"), false);
    backend.insert(createTopicEntity(12L, NS, "asset", AUDIT_INFO), false);
    backend.insert(createFilesetEntity(13L, NS, "asset", AUDIT_INFO), false);
    backend.insert(createModelEntity(14L, NS, "asset", "", 0, Map.of(), AUDIT_INFO), false);
    backend.insert(
        SemanticModelEntity.builder()
            .withId(15L)
            .withName("asset")
            .withNamespace(NS)
            .withAuditInfo(AUDIT_INFO)
            .withProperties(Map.of())
            .withDefinition(
                SemanticModelDefinition.builder()
                    .withDatasets(
                        new Dataset[] {
                          Dataset.builder()
                              .withName("orders")
                              .withSource(NameIdentifier.of("sales", "orders"))
                              .build()
                        })
                    .build())
            .build(),
        false);
    var targets =
        List.of(
            object("TABLE", "orders"),
            object("VIEW", "asset"),
            object("TOPIC", "asset"),
            object("FILESET", "asset"),
            object("MODEL", "asset"),
            object("SEMANTIC_MODEL", "asset"),
            column("orders"),
            ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, List.of(), "cat"),
            ArticleObjectIdentifier.of(MetadataObject.Type.SCHEMA, List.of("cat"), "sales"));
    var initial =
        articles.createArticle(
            LAKE,
            request("guide").addObjects(targets.toArray(ArticleObjectIdentifier[]::new)).build());
    assertEquals(Set.copyOf(targets), new HashSet<>(initial.article().objects()));
    for (int i = 0; i < targets.size(); i++) {
      var page = articles.listAssetBindings(LAKE, "guide", 1, i);
      assertEquals(initial.article().objects().subList(i, i + 1), page.objects());
      assertEquals(i + 1 < targets.size(), page.hasMore());
      var loaded = articles.loadArticle(LAKE, "guide", 1, i).article();
      assertEquals(page.objects(), loaded.objects());
      assertEquals(page.hasMore(), loaded.objectsHasMore());
      assertEquals(
          "guide",
          articles
              .listArticlesForObject(LAKE, targets.get(i), false, 1, 0)
              .articles()
              .get(0)
              .name());
    }
    assertTrue(articles.listAssetBindings(LAKE, "guide", 1, Integer.MAX_VALUE).objects().isEmpty());
    assertThrows(
        IllegalArgumentException.class, () -> articles.listAssetBindings(LAKE, "guide", 0, 0));
    assertEquals(
        initial.revisionToken(),
        articles
            .alterArticle(
                LAKE, "guide", null, KnowledgeBaseArticleChange.addObjects(column("orders")))
            .revisionToken());
  }

  @TestTemplate
  void columnFollowsTableRenameAndNeverRebindsToReplacement() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "cat");
    createAndInsertSchema(LAKE, "cat", "sales");
    var table = table(10, "orders", 11);
    backend.insert(table, false);
    var initial =
        articles.createArticle(LAKE, request("guide").addObjects(column("orders")).build());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            articles.alterArticle(
                LAKE,
                "guide",
                null,
                KnowledgeBaseArticleChange.updateTitle("Changed"),
                KnowledgeBaseArticleChange.removeObjects(column("orders")),
                KnowledgeBaseArticleChange.addObjects(object("TABLE", "missing"))));
    assertEquals(initial.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
    backend.update(table.nameIdentifier(), EntityType.TABLE, old -> table(10, "renamed", 11));
    assertEquals(
        List.of(column("renamed")), articles.loadArticle(LAKE, "guide").article().objects());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> articles.listArticlesForObject(LAKE, column("orders"), false, 1, 0));
    backend.delete(NameIdentifier.of(NS, "renamed"), EntityType.TABLE, false);
    backend.insert(table(20, "renamed", 21), false);
    assertTrue(
        articles.listArticlesForObject(LAKE, column("renamed"), true, 2, 0).articles().isEmpty());
    var page = articles.listAssetBindings(LAKE, "guide", 1, 0);
    assertTrue(page.objects().isEmpty());
    assertFalse(page.hasMore());
    assertEquals(
        initial.revisionToken(),
        articles
            .alterArticle(
                LAKE, "guide", null, KnowledgeBaseArticleChange.removeObjects(column("renamed")))
            .revisionToken());
    var changed =
        articles.alterArticle(
            LAKE,
            "guide",
            initial.revisionToken(),
            KnowledgeBaseArticleChange.updateTitle("Changed"),
            KnowledgeBaseArticleChange.addObjects(column("renamed")));
    assertEquals(2, changed.article().revision());
    assertEquals(List.of(column("renamed")), changed.article().objects());
    assertTrue(
        articles
            .alterArticle(
                LAKE,
                "guide",
                changed.revisionToken(),
                KnowledgeBaseArticleChange.removeObjects(column("renamed")))
            .article()
            .objects()
            .isEmpty());
  }

  @TestTemplate
  void mixedEditsAndReadsAcrossRetiredBindings() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "cat");
    createAndInsertSchema(LAKE, "cat", "sales");
    backend.insert(table(500, "orders", 501), false);
    insertDomain(LAKE, "Sales", Domain.Status.PUBLISHED, null, AUDIT_INFO);
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    glossary.createTerm(LAKE, TermSpec.builder("Cost").build());
    glossary.createTerm(
        LAKE, TermSpec.builder("Draft").withStatus(GlossaryTerm.Status.DRAFT).build());
    var initial = articles.createArticle(LAKE, request("guide").build());
    var domain = KnowledgeBaseArticleChange.updateDomains(new String[] {"Sales"});
    var body =
        KnowledgeBaseArticleChange.updateBody(
            ArticleBody.of(ArticleBody.Format.MARKDOWN, "changed"));
    var asset = KnowledgeBaseArticleChange.addObjects(column("orders"));
    assertThrows(
        UnapprovedTermException.class,
        () ->
            articles.alterArticle(
                LAKE,
                "guide",
                initial.revisionToken(),
                domain,
                asset,
                body,
                KnowledgeBaseArticleChange.addTerms("Draft")));
    assertEquals(initial.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
    assertEquals("body", articles.getArticleContent(LAKE, "guide").body().content());
    var mixed =
        articles.alterArticle(
            LAKE,
            "guide",
            initial.revisionToken(),
            domain,
            asset,
            body,
            KnowledgeBaseArticleChange.addTerms("Cost"));
    assertEquals(2, mixed.article().revision());
    assertEquals(List.of("Sales"), mixed.article().domains());
    assertEquals("Cost", mixed.article().terms().get(0).name());
    assertEquals(List.of(column("orders")), mixed.article().objects());
    var stored = KnowledgeBaseArticleMetaService.getInstance();
    stored.update(
        mixed.article().nameIdentifier(),
        null,
        article -> {
          var bindings = new HashSet<>(article.bindings());
          for (int id = 1; id <= 130; id++) {
            bindings.add(new KnowledgeBaseArticleEntity.Binding("TABLE", id));
          }
          return article.toBuilder().withBindings(bindings).build();
        },
        (before, after) -> {});
    var page = articles.listAssetBindings(LAKE, "guide", 1, 0);
    assertEquals(List.of(column("orders")), page.objects());
    assertFalse(page.hasMore());
    assertTrue(articles.listAssetBindings(LAKE, "guide", 1, 1).objects().isEmpty());
  }

  @TestTemplate
  void replacementBetweenPlanningAndLockRollsBackTheEdit() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "cat");
    createAndInsertSchema(LAKE, "cat", "sales");
    var original = table(10, "orders", 11);
    backend.insert(original, false);
    var guide = articles.createArticle(LAKE, request("guide").build());
    var service = TableMetaService.getInstance();
    var intercepted = Mockito.mock(TableMetaService.class, AdditionalAnswers.delegatesTo(service));
    var peer = Executors.newSingleThreadExecutor();
    try {
      Mockito.doAnswer(
              call -> {
                Long observed = service.getTableIdBySchemaIdAndName(call.getArgument(0), "orders");
                peer.submit(
                        () -> {
                          backend.delete(original.nameIdentifier(), EntityType.TABLE, false);
                          backend.insert(table(20, "orders", 21), false);
                          return null;
                        })
                    .get(20, TimeUnit.SECONDS);
                return observed;
              })
          .when(intercepted)
          .getTableIdBySchemaIdAndName(Mockito.anyLong(), Mockito.eq("orders"));
      try (var services = Mockito.mockStatic(TableMetaService.class)) {
        services.when(TableMetaService::getInstance).thenReturn(intercepted);
        assertThrows(
            NoSuchMetadataObjectException.class,
            () ->
                articles.alterArticle(
                    LAKE,
                    "guide",
                    guide.revisionToken(),
                    KnowledgeBaseArticleChange.updateTitle("Changed"),
                    KnowledgeBaseArticleChange.addObjects(object("TABLE", "orders"))));
      }
      assertEquals(guide.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
      assertTrue(articles.listAssetBindings(LAKE, "guide", 1, 0).objects().isEmpty());
    } finally {
      peer.shutdownNow();
    }
  }

  @TestTemplate
  void latestColumnDeleteHidesOlderVersionsAndSameNameReadd() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "cat");
    createAndInsertSchema(LAKE, "cat", "sales");
    var original = table(10, "orders", 11);
    backend.insert(original, false);
    var guide = articles.createArticle(LAKE, request("guide").addObjects(column("orders")).build());
    var empty =
        TableEntity.builder()
            .withId(10L)
            .withName("orders")
            .withNamespace(NS)
            .withColumns(List.of())
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.update(original.nameIdentifier(), EntityType.TABLE, old -> empty);
    assertTrue(articles.listAssetBindings(LAKE, "guide", 1, 0).objects().isEmpty());
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> articles.listArticlesForObject(LAKE, column("orders"), true, 1, 0));
    backend.update(original.nameIdentifier(), EntityType.TABLE, old -> table(10, "orders", 21));
    assertEquals(guide.revisionToken(), articles.loadArticle(LAKE, "guide").revisionToken());
    assertTrue(
        articles.listArticlesForObject(LAKE, column("orders"), true, 1, 0).articles().isEmpty());
    assertTrue(articles.listAssetBindings(LAKE, "guide", 1, 0).objects().isEmpty());
  }

  private static ArticleCreate.Builder request(String name) {
    return ArticleCreate.builder()
        .withName(name)
        .withTitle(name)
        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"));
  }

  private static TableEntity table(long id, String name, long columnId) {
    var column =
        ColumnEntity.builder()
            .withId(columnId)
            .withName("total.amount")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NS)
        .withColumns(List.of(column))
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private static ArticleObjectIdentifier column(String table) {
    return ArticleObjectIdentifier.of(
        MetadataObject.Type.COLUMN, List.of("cat", "sales", table), "total.amount");
  }

  private static ArticleObjectIdentifier object(String type, String name) {
    return ArticleObjectIdentifier.of(
        MetadataObject.Type.valueOf(type), List.of("cat", "sales"), name);
  }
}
