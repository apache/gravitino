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

package org.apache.gravitino.glossary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Exercises manager transactions through the same entry points used by other core components. */
class TestGlossaryManager extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private final AtomicLong ids = new AtomicLong(100);
  private final GlossaryManager manager = new GlossaryManager(ids::incrementAndGet, false);

  @TestTemplate
  void testMissingParentMakesTermAbsent() {
    TermChange change = TermChange.updateDefinition("new");
    Assertions.assertThrows(NoSuchTermException.class, () -> manager.getTerm(LAKE, "term"));
    Assertions.assertThrows(
        NoSuchTermException.class, () -> manager.alterTerm(LAKE, "term", change));
    Assertions.assertThrows(
        NoSuchTermException.class, () -> manager.alterTerm(LAKE, "term", "bad", change));
    Assertions.assertFalse(manager.dropTerm(LAKE, "term"));
  }

  @TestTemplate
  void testListDistinguishesMissingAndEmptyMetalake() throws IOException {
    NoSuchMetalakeException missing =
        Assertions.assertThrows(NoSuchMetalakeException.class, () -> manager.listTerms(LAKE));
    Assertions.assertTrue(missing.getMessage().contains(LAKE));
    Assertions.assertInstanceOf(NoSuchEntityException.class, missing.getCause());

    createAndInsertMakeLake(LAKE);
    Assertions.assertTrue(manager.listTerms(LAKE).isEmpty());
  }

  @TestTemplate
  void testCreateRequiresExistingMetalake() {
    NoSuchMetalakeException missing =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> manager.createTerm(LAKE, TermSpec.builder("term").build()));
    Assertions.assertTrue(missing.getMessage().contains(LAKE));
    Assertions.assertInstanceOf(NoSuchEntityException.class, missing.getCause());
  }

  @TestTemplate
  void testCreateOwnerAndDropInEveryStatus() throws IOException {
    long lakeId = createAndInsertMakeLake(LAKE).id();
    String user = PrincipalUtils.getCurrentUserName();
    backend.insert(
        createUserEntity(90L, AuthorizationUtils.ofUserNamespace(LAKE), user, AUDIT_INFO), false);
    GlossaryManager owned = new GlossaryManager(ids::incrementAndGet, true);
    GlossaryTermEntity neighbor = manager.createTerm(LAKE, TermSpec.builder("neighbor").build());
    for (GlossaryTerm.Status status : GlossaryTerm.Status.values()) {
      GlossaryTermEntity term =
          owned.createTerm(
              LAKE, TermSpec.builder("a.b/c").withStatus(status).withSynonyms("alias").build());
      Assertions.assertEquals(status, term.status());
      Assertions.assertEquals(
          user,
          OwnerMetaService.getInstance()
              .getOwner(term.nameIdentifier(), EntityType.GLOSSARY_TERM)
              .map(UserEntity.class::cast)
              .orElseThrow()
              .name());
      GlossaryTermRelService relations = GlossaryTermRelService.getInstance();
      relations.add(lakeId, term.id(), neighbor.id(), RelationType.RELATED);
      GlossaryTermEntity renamed =
          owned.alterTerm(LAKE, term.name(), TermChange.rename("new.name"));
      Assertions.assertEquals(term.id(), renamed.id());
      Assertions.assertEquals(Set.of(neighbor.id()), relations.listRelated(lakeId, term.id()));
      Assertions.assertTrue(owned.dropTerm(LAKE, renamed.name()));
      // Looking up the retired term by name would also hide a leaked owner relation.
      Assertions.assertNull(
          SessionUtils.getWithoutCommit(
              OwnerMetaMapper.class,
              mapper ->
                  mapper.selectOwnerByMetadataObjectIdAndType(
                      term.id(), EntityType.GLOSSARY_TERM.name())));
      Assertions.assertFalse(owned.dropTerm(LAKE, renamed.name()));
      Assertions.assertTrue(relations.listRelated(lakeId, neighbor.id()).isEmpty());
    }
    Assertions.assertEquals(1, manager.listTerms(LAKE).size());
  }

  @TestTemplate
  void testCreatorOwnershipRollsBackAndFollowsNewIdentity() throws Exception {
    createAndInsertMakeLake(LAKE);
    GlossaryManager owned = new GlossaryManager(ids::incrementAndGet, true);
    TermSpec spec =
        TermSpec.builder("Account").withSynonyms("client").withSourceRef("import:1").build();
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> PrincipalUtils.doAs(new UserPrincipal("alice"), () -> owned.createTerm(LAKE, spec)));
    Assertions.assertTrue(owned.listTerms(LAKE).isEmpty());

    for (String user : List.of("alice", "bob")) {
      backend.insert(
          createUserEntity(
              ids.incrementAndGet(), AuthorizationUtils.ofUserNamespace(LAKE), user, AUDIT_INFO),
          false);
      GlossaryTermEntity term =
          PrincipalUtils.doAs(new UserPrincipal(user), () -> owned.createTerm(LAKE, spec));
      Assertions.assertEquals(
          user,
          OwnerMetaService.getInstance()
              .getOwner(term.nameIdentifier(), EntityType.GLOSSARY_TERM)
              .map(UserEntity.class::cast)
              .orElseThrow()
              .name());
      Assertions.assertTrue(owned.dropTerm(LAKE, term.name()));
      Assertions.assertNull(
          SessionUtils.getWithoutCommit(
              OwnerMetaMapper.class,
              mapper ->
                  mapper.selectOwnerByMetadataObjectIdAndType(
                      term.id(), EntityType.GLOSSARY_TERM.name())));
    }
  }

  @TestTemplate
  void testReservedNameCannotBeCreatedOrRenamed() throws IOException {
    createAndInsertMakeLake(LAKE);
    String user = PrincipalUtils.getCurrentUserName();
    backend.insert(
        createUserEntity(90L, AuthorizationUtils.ofUserNamespace(LAKE), user, AUDIT_INFO), false);
    for (boolean authorization : List.of(false, true)) {
      GlossaryManager scoped = new GlossaryManager(ids::incrementAndGet, authorization);
      String name = authorization ? "owned" : "unowned";
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> scoped.createTerm(LAKE, TermSpec.builder(" * ").build()));
      GlossaryTermEntity term = scoped.createTerm(LAKE, TermSpec.builder(name).build());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> scoped.alterTerm(LAKE, name, TermChange.rename(" * ")));
      Assertions.assertEquals(term.revisionToken(), scoped.getTerm(LAKE, name).revisionToken());
      if (authorization) {
        Assertions.assertEquals(
            user,
            OwnerMetaService.getInstance()
                .getOwner(term.nameIdentifier(), EntityType.GLOSSARY_TERM)
                .map(UserEntity.class::cast)
                .orElseThrow()
                .name());
      }
    }
    Assertions.assertThrows(NoSuchTermException.class, () -> manager.getTerm(LAKE, "*"));
  }

  @TestTemplate
  void testEffectiveChangesAndConflicts() throws IOException {
    createAndInsertMakeLake(LAKE);
    GlossaryTermEntity term =
        manager.createTerm(LAKE, TermSpec.builder("Customer").withSynonyms("client").build());
    Assertions.assertEquals(GlossaryTerm.Status.APPROVED, term.status());
    Assertions.assertEquals(1, term.version());
    manager.createTerm(
        LAKE, TermSpec.builder("Account").withSynonyms("Client").withSourceRef("import:1").build());
    TermLink a = TermLink.of(null, "HtTpS://example.org/a");
    TermLink b = TermLink.of("b", "http://example.org/b");
    GlossaryTermEntity changed =
        manager.alterTerm(
            LAKE,
            " customer ",
            TermChange.rename("CUSTOMER"),
            TermChange.setStatus(GlossaryTerm.Status.DRAFT),
            TermChange.updateDefinition("new"),
            TermChange.setProperty("team", "A"),
            TermChange.setSourceRef("import:2"),
            TermChange.updateLinks(new TermLink[] {a, b}));
    Assertions.assertEquals(2, changed.version());
    GlossaryTermEntity loaded = manager.getTerm(LAKE, "customer");
    Assertions.assertEquals("CUSTOMER", loaded.name());
    Assertions.assertEquals("new", loaded.definition());
    Assertions.assertEquals(GlossaryTerm.Status.DRAFT, loaded.status());
    Assertions.assertEquals(Map.of("team", "A"), loaded.properties());
    Assertions.assertEquals("import:2", loaded.sourceRef());
    Assertions.assertArrayEquals(new TermLink[] {a, b}, loaded.links());
    Assertions.assertEquals(
        2,
        manager
            .alterTerm(
                LAKE,
                "customer",
                TermChange.addSynonym(" CLIENT "),
                TermChange.removeSynonym("absent"),
                TermChange.removeProperty("team"),
                TermChange.setProperty("team", "A"))
            .version());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.alterTerm(LAKE, "customer", TermChange.addSynonym(" CUSTOMer ")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.alterTerm(LAKE, "customer", TermChange.addSynonym("account")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.createTerm(LAKE, TermSpec.builder(" client ").build()));
    Assertions.assertThrows(
        TermAlreadyExistsException.class,
        () ->
            manager.alterTerm(
                LAKE, "customer", TermChange.removeProperty("team"), TermChange.rename("account")));
    Assertions.assertThrows(
        TermAlreadyExistsException.class,
        () -> manager.alterTerm(LAKE, "customer", TermChange.setSourceRef("import:1")));
    Assertions.assertEquals(2, manager.getTerm(LAKE, "customer").version());
    Assertions.assertEquals(Map.of("team", "A"), manager.getTerm(LAKE, "customer").properties());
    Assertions.assertEquals(
        3,
        manager
            .alterTerm(LAKE, "customer", TermChange.updateLinks(new TermLink[] {b, a}))
            .version());
  }

  @TestTemplate
  void testCancelledSynonymEditPreservesSpelling() throws IOException {
    createAndInsertMakeLake(LAKE);
    String name = "Account";
    String alias = "Active   Customer";
    GlossaryTermEntity created =
        manager.createTerm(LAKE, TermSpec.builder(name).withSynonyms(alias, "Former").build());
    TermChange remove = TermChange.removeSynonym("active customer");
    TermChange restore = TermChange.addSynonym("ACTIVE CUSTOMER");

    GlossaryTermEntity unchanged = manager.alterTerm(LAKE, name, remove, restore);
    Assertions.assertEquals(created.revisionToken(), unchanged.revisionToken());
    Assertions.assertEquals(Set.of(alias, "Former"), Set.of(unchanged.synonyms()));

    GlossaryTermEntity changed =
        manager.alterTerm(
            LAKE,
            name,
            remove,
            restore,
            TermChange.removeSynonym("former"),
            TermChange.addSynonym("New Alias"),
            TermChange.updateDefinition("Updated"));
    Assertions.assertEquals(created.version() + 1, changed.version());
    Assertions.assertEquals(Set.of(alias, "New Alias"), Set.of(changed.synonyms()));
    GlossaryTermEntity loaded = manager.getTerm(LAKE, name);
    Assertions.assertEquals("Updated", loaded.definition());
    Assertions.assertEquals(Set.of(alias, "New Alias"), Set.of(loaded.synonyms()));
  }

  @TestTemplate
  void testConditionalEditAndNameReuse() throws IOException {
    createAndInsertMakeLake(LAKE);
    createAndInsertMakeLake("other");
    String name = "word";
    TermSpec spec = TermSpec.builder(name).build();
    String token = manager.createTerm(LAKE, spec).revisionToken();
    String other = manager.createTerm("other", spec).revisionToken();
    TermChange noop = TermChange.updateDefinition(null);
    Assertions.assertEquals(token, manager.alterTerm(LAKE, name, token, noop).revisionToken());
    Assertions.assertThrows(
        TermVersionConflictException.class, () -> manager.alterTerm(LAKE, name, other, noop));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> manager.alterTerm(LAKE, name, "bad", noop));
    Assertions.assertThrows(
        NoSuchTermException.class, () -> manager.alterTerm(LAKE, "missing", "bad", noop));
    manager.alterTerm(LAKE, name, token, TermChange.rename("new"));
    manager.createTerm(LAKE, spec);
    Assertions.assertThrows(
        TermVersionConflictException.class, () -> manager.alterTerm(LAKE, name, token, noop));
    String replaced = manager.getTerm(LAKE, name).revisionToken();
    manager.dropTerm(LAKE, name);
    manager.createTerm(LAKE, spec);
    Assertions.assertThrows(
        TermVersionConflictException.class, () -> manager.alterTerm(LAKE, name, replaced, noop));
  }

  @TestTemplate
  void testValidationAndBounds() throws IOException {
    createAndInsertMakeLake(LAKE);
    String limit = "😀".repeat(128);
    String definition = "😀".repeat(4096);
    String[] aliases =
        IntStream.range(0, 100).mapToObj(i -> i == 0 ? limit : "alias" + i).toArray(String[]::new);
    TermSpec spec =
        TermSpec.builder("A")
            .withDefinition(definition)
            .withSourceRef(limit)
            .withSynonyms(aliases)
            .build();
    manager.createTerm(LAKE, spec);
    for (String url :
        List.of(
            "data:text/plain,x",
            "javascript:alert(1)",
            "file:///tmp/x",
            "/relative",
            "http:///example",
            "https://a b")) {
      TermLink[] links = {TermLink.of(null, url)};
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> manager.createTerm(LAKE, TermSpec.builder("B").withLinks(links).build()));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> manager.alterTerm(LAKE, "A", TermChange.updateLinks(links)));
    }
    for (TermChange change :
        List.of(
            TermChange.updateDefinition(definition + "a"),
            TermChange.setSourceRef(limit + "a"),
            TermChange.addSynonym("extra"),
            TermChange.addSynonym(limit + "a"),
            TermChange.rename(limit + "a"))) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> manager.alterTerm(LAKE, "A", change));
    }
    for (TermSpec invalid :
        List.of(
            TermSpec.builder(limit + "a").build(),
            TermSpec.builder("B").withDefinition(definition + "a").build(),
            TermSpec.builder("B").withSourceRef(limit + "a").build(),
            TermSpec.builder("B").withSynonyms(limit + "a").build())) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> manager.createTerm(LAKE, invalid));
    }
    Assertions.assertEquals(1, manager.getTerm(LAKE, "A").version());
  }

  @TestTemplate
  void testInputBoundsOnAlterAndRequestRollback() throws IOException {
    createAndInsertMakeLake(LAKE);
    String name = "😀".repeat(128);
    String definition = "😀".repeat(4096);
    String ref = "界".repeat(128);
    String[] synonyms = IntStream.range(0, 101).mapToObj(i -> "alias" + i).toArray(String[]::new);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.createTerm(LAKE, TermSpec.builder("tooMany").withSynonyms(synonyms).build()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.createTerm(LAKE, TermSpec.builder("needsDomain").withDomains("sales").build()));
    String token = manager.createTerm(LAKE, TermSpec.builder("A").build()).revisionToken();
    GlossaryTermEntity term =
        manager.alterTerm(
            LAKE,
            "A",
            token,
            TermChange.rename(name),
            TermChange.updateDefinition(definition),
            TermChange.setSourceRef(ref),
            TermChange.addSynonym("x".repeat(128)));
    Assertions.assertEquals(ref, term.sourceRef());
    Assertions.assertEquals(definition, manager.getTerm(LAKE, name).definition());
    Assertions.assertThrows(
        TermVersionConflictException.class,
        () ->
            manager.alterTerm(
                LAKE,
                name,
                token,
                TermChange.setSourceRef(ref),
                TermChange.updateDefinition(definition)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            manager.alterTerm(
                LAKE,
                name,
                TermChange.updateDefinition(definition + "a"),
                TermChange.updateDefinition(definition)));
    Assertions.assertThrows(
        NoSuchTermException.class,
        () ->
            manager.alterTerm(
                LAKE, name, TermChange.setProperty("x", "y"), TermChange.addBroader("someTerm")));
    Assertions.assertEquals(term.revisionToken(), manager.getTerm(LAKE, name).revisionToken());
    Assertions.assertTrue(manager.getTerm(LAKE, name).properties().isEmpty());
  }

  @TestTemplate
  void testHandlerAndRollback() throws IOException {
    createAndInsertMakeLake(LAKE);
    List<String> calls = new ArrayList<>();
    GlossaryManager observed =
        new GlossaryManager(
            ids::incrementAndGet,
            false,
            (before, after, persist) -> {
              calls.add(before == null ? "create" : after == null ? "drop" : "alter");
              persist.run();
              if (after != null && after.status() == GlossaryTerm.Status.DEPRECATED) {
                throw new IllegalStateException("Rejected");
              }
            });
    observed.createTerm(LAKE, TermSpec.builder("A").build());
    observed.alterTerm(LAKE, "A", TermChange.setStatus(GlossaryTerm.Status.APPROVED));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> observed.alterTerm(LAKE, "A", TermChange.setStatus(GlossaryTerm.Status.DEPRECATED)));
    Assertions.assertEquals(1, observed.getTerm(LAKE, "A").version());
    observed.dropTerm(LAKE, "A");
    Assertions.assertEquals(List.of("create", "alter", "drop"), calls);
  }

  @TestTemplate
  void testConcurrentNameAndSynonymChecks() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (boolean rename : List.of(false, true)) {
      for (boolean initialSynonym : List.of(false, true)) {
        for (boolean nameWins : List.of(false, true)) {
          String suffix = Long.toString(ids.incrementAndGet());
          String target = "X" + suffix;
          String alias = " x" + suffix + " ";
          String original = "original" + suffix;
          String other = "other" + suffix;
          if (rename) manager.createTerm(LAKE, TermSpec.builder(original).build());
          if (!initialSynonym) manager.createTerm(LAKE, TermSpec.builder(other).build());
          race(
              (m) ->
                  rename
                      ? m.alterTerm(LAKE, original, TermChange.rename(target))
                      : m.createTerm(LAKE, TermSpec.builder(target).build()),
              (m) ->
                  initialSynonym
                      ? m.createTerm(LAKE, TermSpec.builder(other).withSynonyms(alias).build())
                      : m.alterTerm(LAKE, other, TermChange.addSynonym(alias)),
              nameWins,
              IllegalArgumentException.class);
          if (nameWins) {
            Assertions.assertEquals(target, manager.getTerm(LAKE, target).name());
          } else {
            Assertions.assertEquals(alias.trim(), manager.getTerm(LAKE, other).synonyms()[0]);
            if (rename) Assertions.assertEquals(1, manager.getTerm(LAKE, original).version());
          }
        }
      }
    }
  }

  @TestTemplate
  void testConcurrentRevisions() throws Exception {
    createAndInsertMakeLake(LAKE);
    String token = manager.createTerm(LAKE, TermSpec.builder("A").build()).revisionToken();
    race(
        m ->
            m.alterTerm(
                LAKE,
                "A",
                token,
                TermChange.updateLinks(new TermLink[] {TermLink.of(null, "https://one.org")})),
        m ->
            m.alterTerm(
                LAKE,
                "A",
                token,
                TermChange.updateLinks(new TermLink[] {TermLink.of(null, "https://two.org")})),
        true,
        TermVersionConflictException.class);
    race(
        m -> m.alterTerm(LAKE, "A", TermChange.addSynonym("one")),
        m -> m.alterTerm(LAKE, "A", TermChange.addSynonym("two")),
        true,
        null);
    Assertions.assertEquals(4, manager.getTerm(LAKE, "A").version());
    Assertions.assertArrayEquals(
        new String[] {"one", "two"}, manager.getTerm(LAKE, "A").synonyms());
  }

  private void race(
      Function<GlossaryManager, GlossaryTermEntity> first,
      Function<GlossaryManager, GlossaryTermEntity> second,
      boolean firstWins,
      @Nullable Class<? extends Throwable> failure)
      throws Exception {
    CountDownLatch checked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    GlossaryManager winner =
        new GlossaryManager(
            ids::incrementAndGet,
            false,
            (before, after, persist) -> {
              checked.countDown();
              try {
                Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              persist.run();
            });
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> winning = pool.submit(() -> (firstWins ? first : second).apply(winner));
      Assertions.assertTrue(checked.await(10, TimeUnit.SECONDS));
      Future<?> waiting =
          pool.submit(
              () -> {
                started.countDown();
                return (firstWins ? second : first).apply(manager);
              });
      Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
      Assertions.assertThrows(
          TimeoutException.class, () -> waiting.get(150, TimeUnit.MILLISECONDS));
      release.countDown();
      winning.get(10, TimeUnit.SECONDS);
      if (failure == null) waiting.get(10, TimeUnit.SECONDS);
      else
        Assertions.assertInstanceOf(
            failure,
            Assertions.assertThrows(
                    ExecutionException.class, () -> waiting.get(10, TimeUnit.SECONDS))
                .getCause());
    } finally {
      release.countDown();
      pool.shutdownNow();
      Assertions.assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
