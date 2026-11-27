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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.SchemaOperationDispatcher;
import org.apache.gravitino.catalog.TableOperationDispatcher;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.glossary.TermSpec;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.OrphanedMetadataObjectRelationMapper;
import org.apache.gravitino.storage.relational.mapper.OrphanedMetadataObjectRelationMapper.ArticleBindingCandidate;
import org.apache.gravitino.storage.relational.mapper.OrphanedMetadataObjectRelationMapper.RetiredTarget;
import org.apache.gravitino.storage.relational.service.KnowledgeBaseArticleMetaService;
import org.apache.gravitino.storage.relational.service.OrphanedMetadataObjectRelationService;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

class TestArticleBindingCleanup extends TestJDBCBackend {
  private static final String LAKE = "lake";
  private static final Namespace NS = Namespace.of(LAKE, "warehouse", "sales");
  private final AtomicLong ids = new AtomicLong(100);
  private final ArticleManager articles = new ArticleManager(ids::incrementAndGet, 1024);

  @TestTemplate
  void auditsRetiredTargetsOnlyAfterCommit() throws Exception {
    long lake = createAndInsertMakeLake(LAKE).id();
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    long term = glossary.createTerm(LAKE, TermSpec.builder("Revenue").build()).id();
    articles.createArticle(LAKE, request("first").addTerms("Revenue").build());
    articles.createArticle(LAKE, request("second").addTerms("Revenue").build());
    assertTrue(glossary.dropTerm(LAKE, "Revenue"));

    var context = (LoggerContext) LogManager.getContext(false);
    var logger = context.getLogger(OrphanedMetadataObjectRelationService.class.getName());
    Level previous = logger.getLevel();
    StringWriter logs = new StringWriter();
    WriterAppender appender =
        WriterAppender.newBuilder()
            .setName("articleCleanup")
            .setTarget(logs)
            .setLayout(PatternLayout.newBuilder().withPattern("%m%n").build())
            .build();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      SessionUtils.beginTransaction();
      try {
        assertEquals(2, sweep(MetadataObject.Type.GLOSSARY_TERM, 10));
        assertTrue(logs.toString().isEmpty());
      } finally {
        SessionUtils.rollbackTransaction();
      }
      assertTrue(logs.toString().isEmpty());
      assertEquals(2, sweep(MetadataObject.Type.GLOSSARY_TERM, 10));
      assertEquals(0, sweep(MetadataObject.Type.GLOSSARY_TERM, 10));
      assertEquals(
          List.of(
              "Article binding cleanup actor=system metalakeId="
                  + lake
                  + " targetType=GLOSSARY_TERM targetId="
                  + term
                  + " removedRows=2"),
          logs.toString().lines().toList());
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @TestTemplate
  void concurrentSweepsWithTheSameTimestampReportOnlyTheirOwnRetirements() throws Exception {
    long lake = createAndInsertMakeLake(LAKE).id();
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    long first = glossary.createTerm(LAKE, TermSpec.builder("Revenue").build()).id();
    long second = glossary.createTerm(LAKE, TermSpec.builder("Tax").build()).id();
    articles.createArticle(LAKE, request("one").addTerms("Revenue").build());
    articles.createArticle(LAKE, request("two").addTerms("Tax").build());
    assertTrue(glossary.dropTerm(LAKE, "Revenue"));
    assertTrue(glossary.dropTerm(LAKE, "Tax"));

    var sessions = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory();
    long deletedAt = System.currentTimeMillis();
    var worker = Executors.newSingleThreadExecutor();
    try (var firstSession = sessions.openSession(TransactionIsolationLevel.READ_COMMITTED)) {
      var mapper = firstSession.getMapper(OrphanedMetadataObjectRelationMapper.class);
      String table = "glossary_term_meta";
      String type = "GLOSSARY_TERM";
      var candidates =
          mapper.articleBindingCandidates(table, "term_id", type, 0, Long.MAX_VALUE, 10).stream()
              .filter(ArticleBindingCandidate::orphaned)
              .map(ArticleBindingCandidate::id)
              .toList();
      var ids = mapper.lockLiveArticleBindingIds(candidates.subList(0, 1));
      assertEquals(1, mapper.retireOrphanedArticleBindings(table, "term_id", type, ids, deletedAt));
      assertEquals(
          List.of(new RetiredTarget(lake, first, 1)),
          mapper.listRetiredArticleTargets(ids, deletedAt));

      var started = new CountDownLatch(1);
      var remaining =
          worker.submit(
              () -> {
                try (var session = sessions.openSession(TransactionIsolationLevel.READ_COMMITTED)) {
                  var concurrent = session.getMapper(OrphanedMetadataObjectRelationMapper.class);
                  started.countDown();
                  var batch = concurrent.lockLiveArticleBindingIds(candidates);
                  assertEquals(
                      1,
                      concurrent.retireOrphanedArticleBindings(
                          table, "term_id", type, batch, deletedAt));
                  var targets = concurrent.listRetiredArticleTargets(batch, deletedAt);
                  session.commit();
                  return targets;
                }
              });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> remaining.get(150, TimeUnit.MILLISECONDS));
      firstSession.commit();
      assertEquals(
          List.of(new RetiredTarget(lake, second, 1)), remaining.get(10, TimeUnit.SECONDS));
    } finally {
      worker.shutdownNow();
    }
  }

  @TestTemplate
  void termRetirementIsBatchedByBindingAndPreservesArticles() throws Exception {
    createAndInsertMakeLake(LAKE);
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    glossary.createTerm(LAKE, TermSpec.builder("Cost").build());
    var first = articles.createArticle(LAKE, request("first").addTerms("Cost").build());
    var second = articles.createArticle(LAKE, request("second").addTerms("Cost").build());
    glossary.alterTerm(LAKE, "Cost", TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
    assertEquals(0, sweep(MetadataObject.Type.GLOSSARY_TERM, 1));
    assertTrue(glossary.dropTerm(LAKE, "Cost"));
    glossary.createTerm(LAKE, TermSpec.builder("Cost").build());
    var edited =
        articles.alterArticle(
            LAKE, "first", first.revisionToken(), KnowledgeBaseArticleChange.addTerms("Cost"));
    assertEquals(
        3, countActiveRowsWhere("kb_article_relation_meta", "target_type='GLOSSARY_TERM'"));
    assertEquals(1, sweep(MetadataObject.Type.GLOSSARY_TERM, 1));
    assertEquals(
        2, countActiveRowsWhere("kb_article_relation_meta", "target_type='GLOSSARY_TERM'"));
    assertEquals(1, sweep(MetadataObject.Type.GLOSSARY_TERM, 1));
    assertEquals(0, sweep(MetadataObject.Type.GLOSSARY_TERM, 1));
    assertEquals(
        1, countActiveRowsWhere("kb_article_relation_meta", "target_type='GLOSSARY_TERM'"));
    assertEquals(edited.revisionToken(), articles.loadArticle(LAKE, "first").revisionToken());
    assertEquals(second.revisionToken(), articles.loadArticle(LAKE, "second").revisionToken());
    assertEquals("body", articles.getArticleContent(LAKE, "first").body().content());
    assertEquals(
        "first", articles.listArticlesForTerm(LAKE, "Cost", false, 10, 0).articles().get(0).name());
    assertEquals(2, KnowledgeBaseArticleMetaService.getInstance().collect(Long.MAX_VALUE, 10));
    assertEquals(0, KnowledgeBaseArticleMetaService.getInstance().collect(Long.MAX_VALUE, 10));
  }

  @TestTemplate
  void sweepContinuesPastLivePagesAndRestartsFromTheBeginning() throws Exception {
    createAndInsertMakeLake(LAKE);
    var glossary = new GlossaryManager(ids::incrementAndGet, false);
    glossary.createTerm(LAKE, TermSpec.builder("Kept").build());
    glossary.createTerm(LAKE, TermSpec.builder("Dropped").build());
    articles.createArticle(LAKE, request("first").addTerms("Kept").build());
    articles.createArticle(LAKE, request("second").addTerms("Kept").build());
    articles.createArticle(LAKE, request("third").addTerms("Dropped").build());
    assertTrue(glossary.dropTerm(LAKE, "Dropped"));
    var cleanup = OrphanedMetadataObjectRelationService.getInstance();
    cleanup.sweepOrphanedRelations(MetadataObject.Type.GLOSSARY_TERM, 1);
    assertEquals(
        2, countActiveRowsWhere("kb_article_relation_meta", "target_type='GLOSSARY_TERM'"));
    assertTrue(glossary.dropTerm(LAKE, "Kept"));
    cleanup.sweepOrphanedRelations(MetadataObject.Type.GLOSSARY_TERM, 1);
    assertEquals(
        0, countActiveRowsWhere("kb_article_relation_meta", "target_type='GLOSSARY_TERM'"));
  }

  @TestTemplate
  void cascadeRetiresOnlyBindingsToDeletedEndpoints() throws Exception {
    createAndInsertMakeLake(LAKE);
    var catalog = createAndInsertCatalog(LAKE, "warehouse");
    createAndInsertSchema(LAKE, "warehouse", "sales");
    createAndInsertTableEntity(NS, "orders");
    createAndInsertCatalog(LAKE, "keep");
    var keep = ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, List.of(), "keep");
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

    List<ArticleObjectIdentifier> targets =
        new ArrayList<>(
            List.of(
                ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, List.of(), "warehouse"),
                ArticleObjectIdentifier.of(
                    MetadataObject.Type.SCHEMA, List.of("warehouse"), "sales"),
                table()));
    for (var type :
        List.of(
            MetadataObject.Type.VIEW,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.SEMANTIC_MODEL)) {
      targets.add(ArticleObjectIdentifier.of(type, List.of("warehouse", "sales"), "asset"));
    }
    var initial =
        articles.createArticle(
            LAKE,
            request("guide")
                .addObjects(targets.toArray(ArticleObjectIdentifier[]::new))
                .addObjects(keep)
                .build());
    for (var target : targets) {
      assertEquals(0, sweep(target.type(), 1));
    }
    assertTrue(backend.delete(catalog.nameIdentifier(), EntityType.CATALOG, true));
    for (var target : targets) {
      var type = target.type();
      assertEquals(1, sweep(type, 1));
      assertEquals(0, sweep(type, 1));
    }
    var result = articles.loadArticle(LAKE, "guide");
    assertEquals(initial.revisionToken(), result.revisionToken());
    assertEquals(List.of(keep), result.article().objects());
    assertEquals(
        1,
        countActiveRowsWhere("kb_article_relation_meta", "article_id=" + initial.article().id()));
  }

  @TestTemplate
  void staleBatchRechecksTargetAndLeavesNewBindingsAlone() throws Exception {
    createAndInsertMakeLake(LAKE);
    createAndInsertCatalog(LAKE, "warehouse");
    createAndInsertSchema(LAKE, "warehouse", "sales");
    var original = createTableEntity(42L, NS, "orders", AUDIT_INFO);
    backend.insert(original, false);
    var guide = articles.createArticle(LAKE, request("guide").addObjects(table()).build());
    assertTrue(backend.delete(original.nameIdentifier(), EntityType.TABLE, false));
    // Also select an orphan whose stable ID gets registered again before the update.
    KnowledgeBaseArticleMetaService.getInstance()
        .update(
            guide.article().nameIdentifier(),
            null,
            article ->
                article.toBuilder()
                    .withBindings(Set.of(new Binding("TABLE", 42), new Binding("TABLE", 77)))
                    .build(),
            (before, after) -> {});
    // Leave the sweep transaction open while independent connections register and bind the targets.
    try (var sweepSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
      var mapper = sweepSession.getMapper(OrphanedMetadataObjectRelationMapper.class);
      var candidates =
          mapper
              .articleBindingCandidates("table_meta", "table_id", "TABLE", 0, Long.MAX_VALUE, 10)
              .stream()
              .filter(ArticleBindingCandidate::orphaned)
              .map(ArticleBindingCandidate::id)
              .toList();
      assertEquals(2, candidates.size());
      backend.insert(createTableEntity(99L, NS, "orders", AUDIT_INFO), false);
      backend.insert(createTableEntity(77L, NS, "restored", AUDIT_INFO), false);
      articles.alterArticle(LAKE, "guide", null, KnowledgeBaseArticleChange.addObjects(table()));
      long deletedAt = System.currentTimeMillis();
      assertEquals(
          1,
          mapper.retireOrphanedArticleBindings(
              "table_meta", "table_id", "TABLE", candidates, deletedAt));
      assertEquals(
          List.of(42L),
          mapper.listRetiredArticleTargets(candidates, deletedAt).stream()
              .map(OrphanedMetadataObjectRelationMapper.RetiredTarget::targetId)
              .toList());
      sweepSession.commit();
    }
    assertEquals(
        Set.of(new Binding("TABLE", 77), new Binding("TABLE", 99)),
        KnowledgeBaseArticleMetaService.getInstance()
            .getScoped(guide.article().nameIdentifier())
            .article()
            .bindings());
    assertEquals(0, sweep(MetadataObject.Type.TABLE, 10));
  }

  @TestTemplate
  void externalColumnChangesBecomeCollectibleAfterLoadReconcilesTheRegistration() throws Exception {
    createAndInsertMakeLake(LAKE);
    var config = new Config(false) {};
    config.set(Configs.CATALOG_LOAD_ISOLATED, false);
    // The fixture owns the backend. Expose the store without subscribing to a background poller.
    var store =
        Mockito.mock(
            EntityStore.class,
            AdditionalAnswers.delegatesTo(
                RelationalEntityStore.forOfflineMigration(backend, config)));
    var env = GravitinoEnv.getInstance();
    var previousLocks = FieldUtils.readField(env, "lockManager", true);
    var previousCatalogs = FieldUtils.readField(env, "catalogManager", true);
    try (var secrets = new SecretManager(config);
        var catalogs = new CatalogManager(config, store, ids::incrementAndGet, secrets)) {
      FieldUtils.writeField(env, "lockManager", new LockManager(config), true);
      FieldUtils.writeField(env, "catalogManager", catalogs, true);
      var catalog = NameIdentifier.of(LAKE, "warehouse");
      catalogs.createCatalog(
          catalog,
          Catalog.Type.RELATIONAL,
          "test",
          "",
          Map.of(
              TestCatalog.PROPERTY_KEY1,
              "v1",
              TestCatalog.PROPERTY_KEY2,
              "v2",
              TestCatalog.PROPERTY_KEY5_PREFIX + "1",
              "v3"));
      var schemas = new SchemaOperationDispatcher(catalogs, store, ids::incrementAndGet, secrets);
      var tables =
          new TableOperationDispatcher(
              catalogs, store, ids::incrementAndGet, () -> schemas, secrets);
      var table = NameIdentifier.of(NS, "orders");
      var props = Map.of("k1", "v1", "k2", "v2");
      schemas.createSchema(NameIdentifier.of(NS.levels()), "", props);
      var source = catalogs.doWithCatalog(catalog, c -> (TestCatalogOperations) c.ops());
      source.createTable(
          table,
          new Column[] {
            Column.of("kept", Types.IntegerType.get()), Column.of("legacy", Types.IntegerType.get())
          },
          "",
          props,
          new Transform[0]);
      tables.loadTable(table);
      var names = List.of("warehouse", "sales", "orders");
      var kept = ArticleObjectIdentifier.of(MetadataObject.Type.COLUMN, names, "kept");
      var removed = ArticleObjectIdentifier.of(MetadataObject.Type.COLUMN, names, "legacy");
      var initial =
          articles.createArticle(LAKE, request("guide").addObjects(kept, removed).build());

      source.alterTable(table, TableChange.deleteColumn(new String[] {"legacy"}, false));
      assertEquals(0, sweep(MetadataObject.Type.COLUMN, 10));
      tables.loadTable(table);
      assertEquals(List.of(kept), articles.loadArticle(LAKE, "guide").article().objects());
      assertEquals(2, countActiveRowsWhere("kb_article_relation_meta", "target_type='COLUMN'"));
      source.alterTable(
          table, TableChange.addColumn(new String[] {"legacy"}, Types.IntegerType.get()));
      tables.loadTable(table);
      assertTrue(articles.listArticlesForObject(LAKE, removed, false, 1, 0).articles().isEmpty());
      assertEquals(1, sweep(MetadataObject.Type.COLUMN, 10));
      assertEquals(0, sweep(MetadataObject.Type.COLUMN, 10));
      assertEquals(1, countActiveRowsWhere("kb_article_relation_meta", "target_type='COLUMN'"));
      var after = articles.loadArticle(LAKE, "guide");
      assertEquals(initial.revisionToken(), after.revisionToken());
      assertEquals(List.of(kept), after.article().objects());
    } finally {
      FieldUtils.writeField(env, "lockManager", previousLocks, true);
      FieldUtils.writeField(env, "catalogManager", previousCatalogs, true);
    }
  }

  private static ArticleObjectIdentifier table() {
    return ArticleObjectIdentifier.of(
        MetadataObject.Type.TABLE, List.of("warehouse", "sales"), "orders");
  }

  private static ArticleCreate.Builder request(String name) {
    return ArticleCreate.builder()
        .withName(name)
        .withTitle(name)
        .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"));
  }

  private static int sweep(MetadataObject.Type type, int limit) {
    return OrphanedMetadataObjectRelationService.getInstance()
        .softDeleteOrphanedRelations(type, limit);
  }
}
