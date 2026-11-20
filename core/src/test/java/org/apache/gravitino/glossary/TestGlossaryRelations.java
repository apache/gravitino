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

import static org.apache.gravitino.glossary.GlossaryManager.Direction.BROADER;
import static org.apache.gravitino.glossary.GlossaryManager.Direction.NARROWER;
import static org.apache.gravitino.glossary.GlossaryManager.Direction.RELATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.gravitino.exceptions.NoSuchTermException;
import org.apache.gravitino.exceptions.TermAlreadyExistsException;
import org.apache.gravitino.exceptions.TermVersionConflictException;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.service.GlossaryInvariants;
import org.apache.gravitino.storage.relational.service.GlossaryTermRelService.RelationType;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestGlossaryRelations extends TestJDBCBackend {
  private static final String LAKE = "relations";
  private final AtomicLong ids = new AtomicLong(1000);
  private final GlossaryManager manager = new GlossaryManager(ids::incrementAndGet, false);

  @AfterEach
  void verifyStorageInvariants() throws Exception {
    GlossaryInvariants.assertDefinitions();
    GlossaryInvariants.assertRelations();
  }

  /** Resolves referenced names in bounded batches before taking the ordered term locks. */
  @TestTemplate
  void testBatchRelationTargetLookup() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("Root", "Parent", "Peer");
    List<Integer> lookupSizes = new ArrayList<>();
    try (MockedStatic<SqlSessions> sessions =
        Mockito.mockStatic(
            SqlSessions.class,
            call -> {
              Object result = call.callRealMethod();
              if (!call.getMethod().getName().equals("getMapper")
                  || call.getArgument(0) != GlossaryTermMetaMapper.class) {
                return result;
              }
              return Mockito.mock(
                  GlossaryTermMetaMapper.class,
                  query -> {
                    switch (query.getMethod().getName()) {
                      case "idsByKeys" -> lookupSizes.add(
                          query.<List<String>>getArgument(1).size());
                      case "getIds" -> lookupSizes.add(1);
                      default -> {}
                    }
                    return AdditionalAnswers.delegatesTo(result).answer(query);
                  });
            })) {
      edit(
          "Root",
          TermChange.addBroader("parent"),
          TermChange.addBroader(" PARENT "),
          TermChange.addRelated("peer"));
      assertEquals(List.of(2), lookupSizes);
      assertEquals(
          List.of("Parent"), names(manager.listRelations(LAKE, "Root", BROADER, false, null)));
      assertEquals(
          List.of("Peer"), names(manager.listRelations(LAKE, "Root", RELATED, false, null)));
      assertEquals(List.of(2), lookupSizes, "Relation reads must reuse the root snapshot IDs");
      lookupSizes.clear();
      List<TermChange> edits = new ArrayList<>();
      edits.add(TermChange.removeBroader("Parent"));
      for (int i = 0; i < 500; i++) {
        edits.add(TermChange.addRelated("missing" + i));
      }
      String token = get("Root").revisionToken();
      assertThrows(NoSuchTermException.class, () -> edit("Root", edits.toArray(new TermChange[0])));
      assertEquals(List.of(500, 1), lookupSizes);
      assertEquals(token, get("Root").revisionToken());
      assertEquals(
          List.of("Parent"), names(manager.listRelations(LAKE, "Root", BROADER, false, null)));
    }
  }

  /** Covers T5.1, T5.2 and T5.3. */
  @TestTemplate
  void testFinalGraphAndRevisions() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("A", "B", "C", "D");
    edit("A", TermChange.addBroader("B"));
    edit("B", TermChange.addBroader("C"));
    edit("C", TermChange.addBroader("D"));
    for (TermChange invalid :
        List.of(
            TermChange.addBroader("A"), TermChange.addBroader("C"), TermChange.addRelated("A"))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> edit("D", TermChange.updateDefinition("invalid"), invalid));
      assertEquals(1, get("D").version());
    }
    assertThrows(IllegalArgumentException.class, () -> edit("A", TermChange.addRelated("D")));
    assertThrows(IllegalArgumentException.class, () -> edit("A", TermChange.addRelated("a")));
    assertThrows(
        IllegalArgumentException.class,
        () -> manager.createTerm(LAKE, TermSpec.builder("Self").withBroader(" SELF ").build()));
    assertThrows(NoSuchTermException.class, () -> get("Self"));
    assertThrows(
        NoSuchTermException.class,
        () -> manager.createTerm(LAKE, TermSpec.builder("New").withRelated("absent").build()));
    assertThrows(NoSuchTermException.class, () -> get("New"));
    long revision = get("A").version();
    edit("A", TermChange.removeBroader(" b "), TermChange.addBroader("B"));
    assertEquals(revision, get("A").version());
    edit(
        "A",
        TermChange.addRelated("B"),
        TermChange.removeBroader("B"),
        TermChange.updateDefinition("mixed"));
    assertEquals(revision + 1, get("A").version());
    assertEquals(List.of("A"), names(manager.listRelations(LAKE, "B", RELATED, false, null)));
    assertEquals(2, get("B").version());
  }

  /** Covers T5.6 and T5.7, including a change that removes an existing conflict. */
  @TestTemplate
  void testRelatedAndAncestorConflictAtRoot() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("A", "B", "C");
    edit("B", TermChange.addBroader("C"));
    edit("A", TermChange.addRelated("C"));
    String token = get("A").revisionToken();
    assertThrows(IllegalArgumentException.class, () -> edit("A", TermChange.addBroader("B")));
    assertEquals(token, get("A").revisionToken());
    assertEquals(List.of("C"), names(manager.listRelations(LAKE, "A", RELATED, false, null)));
    edit("A", TermChange.addBroader("B"), TermChange.removeRelated("C"));
    assertThrows(IllegalArgumentException.class, () -> edit("C", TermChange.addRelated("A")));
    assertThrows(IllegalArgumentException.class, () -> edit("A", TermChange.addRelated("C")));
    assertEquals(List.of("B", "C"), names(manager.listRelations(LAKE, "A", BROADER, true, null)));
  }

  /** Covers T5.15. */
  @TestTemplate
  void testCrossingRelatedBothIdOrders() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (int i = 0; i < 2; i++) {
      String x = "x" + i, y = "y" + i, a = "a" + i, b = "b" + i;
      if (i == 0) create(x, y);
      else create(y, x);
      create(a, b);
      edit(x, TermChange.addBroader(a), TermChange.addRelated(y));
      edit(b, TermChange.addBroader(y));
      assertThrows(
          IllegalArgumentException.class,
          () -> edit(a, TermChange.rename("renamed"), TermChange.addBroader(b)));
      assertEquals(1, get(a).version());
      assertThrows(NoSuchTermException.class, () -> get("renamed"));
    }
  }

  /** Covers T5.4 and T5.5. */
  @TestTemplate
  void testDepthAcrossBothSides() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (int limit : List.of(10, 20, 50)) {
      GlossaryManager bounded =
          new GlossaryManager(ids::incrementAndGet, false, (b, a, p) -> p.run(), t -> true, limit);
      String prefix = "depth" + limit + "-";
      for (int i = 0; i <= limit + 1; i++) {
        TermSpec.Builder spec = TermSpec.builder(prefix + i);
        if (i > 0 && i != limit / 2 + 1) spec.withBroader(prefix + (i - 1));
        bounded.createTerm(LAKE, spec.build());
      }
      String bottom = prefix + (limit / 2 + 1);
      assertThrows(
          IllegalArgumentException.class,
          () -> bounded.alterTerm(LAKE, bottom, TermChange.addBroader(prefix + limit / 2)));
      bounded.alterTerm(LAKE, bottom, TermChange.addBroader(prefix + (limit / 2 - 1)));
      assertEquals(
          limit, bounded.listRelations(LAKE, prefix + (limit + 1), BROADER, true, null).size());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              bounded.createTerm(
                  LAKE,
                  TermSpec.builder(prefix + "last").withBroader(prefix + (limit + 1)).build()));
      String above = prefix + "above";
      bounded.createTerm(LAKE, TermSpec.builder(above).build());
      assertThrows(
          IllegalArgumentException.class,
          () -> bounded.alterTerm(LAKE, prefix + 0, TermChange.addBroader(above)));
    }
  }

  /** Covers T5.11, T5.12 and the visibility predicate in T5.13. */
  @TestTemplate
  void testVisibilityAtEveryFrontier() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("top");
    for (String name : List.of("left", "right"))
      manager.createTerm(LAKE, TermSpec.builder(name).withBroader("top").build());
    manager.createTerm(LAKE, TermSpec.builder("base").withBroader("left", "right").build());
    assertEquals(
        Set.of("top", "left", "right"),
        new HashSet<>(names(manager.listRelations(LAKE, "base", BROADER, true, null))));
    assertEquals(3, manager.listRelations(LAKE, "top", NARROWER, true, null).size());
    GlossaryManager filtered =
        manager.withTermVisibility(t -> !Set.of("left", "right").contains(t.name()));
    assertTrue(filtered.listRelations(LAKE, "base", BROADER, true, null).isEmpty());
    assertThrows(
        NoSuchTermException.class, () -> filtered.listRelations(LAKE, "left", BROADER, true, null));
    assertThrows(
        NoSuchTermException.class,
        () -> filtered.alterTerm(LAKE, "top", TermChange.addRelated("left")));
    assertThrows(
        NoSuchTermException.class,
        () -> filtered.alterTerm(LAKE, "left", "malformed", TermChange.addRelated("top")));
    edit("left", TermChange.setStatus(GlossaryTerm.Status.DRAFT));
    edit("right", TermChange.setStatus(GlossaryTerm.Status.DRAFT));
    assertTrue(
        manager.listRelations(LAKE, "top", NARROWER, true, GlossaryTerm.Status.APPROVED).isEmpty());
    assertThrows(
        NoSuchTermException.class,
        () -> manager.listRelations(LAKE, "left", BROADER, true, GlossaryTerm.Status.APPROVED));
  }

  @TestTemplate
  void testInvisibleRelationRootUsesRequestedName() throws Exception {
    createAndInsertMakeLake(LAKE);
    String alias = "dRaFt";
    String missing =
        assertThrows(
                NoSuchTermException.class,
                () -> manager.listRelations(LAKE, alias, BROADER, true, null))
            .getMessage();
    create("Draft", "Peer");
    edit("Draft", TermChange.setStatus(GlossaryTerm.Status.DRAFT));
    GlossaryManager filtered = manager.withTermVisibility(term -> !term.name().equals("Draft"));
    assertEquals(
        missing,
        assertThrows(
                NoSuchTermException.class,
                () -> filtered.listRelations(LAKE, alias, BROADER, true, null))
            .getMessage());
    assertEquals(
        missing,
        assertThrows(
                NoSuchTermException.class,
                () ->
                    manager.listRelations(LAKE, alias, BROADER, true, GlossaryTerm.Status.APPROVED))
            .getMessage());
    assertEquals(
        missing,
        assertThrows(
                NoSuchTermException.class,
                () -> filtered.alterTerm(LAKE, alias, "invalid", TermChange.addRelated("Peer")))
            .getMessage());
  }

  /** Covers T5.10 and T5.17. */
  @TestTemplate
  void testCleanupRevisionsAndRollback() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("A", "B", "C", "D", "E");
    edit("A", TermChange.addBroader("B"), TermChange.addBroader("C"));
    edit(
        "B",
        TermChange.addBroader("C"),
        TermChange.addRelated("D"),
        TermChange.setStatus(GlossaryTerm.Status.DEPRECATED));
    edit("E", TermChange.addRelated("B"));
    List<String> losses = new ArrayList<>();
    TermChangeHandler audit =
        new TermChangeHandler() {
          @Override
          public void handle(
              GlossaryTermEntity before, GlossaryTermEntity after, Runnable persist) {
            persist.run();
          }

          @Override
          public void handleRelationLoss(
              GlossaryTermEntity before,
              GlossaryTermEntity after,
              GlossaryTermEntity removed,
              RelationType type,
              Runnable persist) {
            persist.run();
            losses.add(after.name() + "/" + removed.name() + "/" + type + "/" + after.version());
            if (losses.size() == 2) throw new IllegalStateException("abort cleanup");
          }
        };
    GlossaryManager observed = new GlossaryManager(ids::incrementAndGet, false, audit);
    assertThrows(IllegalStateException.class, () -> observed.dropTerm(LAKE, "B"));
    assertEquals(2, get("A").version());
    assertEquals(1, get("D").version());
    assertEquals(2, manager.listRelations(LAKE, "B", RELATED, false, null).size());
    assertTrue(observed.dropTerm(LAKE, "B"));
    assertEquals(
        Set.of("A/B/BROADER/3", "D/B/RELATED/2", "E/B/RELATED/3"),
        new HashSet<>(losses.subList(2, losses.size())));
    assertEquals(List.of("C"), names(manager.listRelations(LAKE, "A", BROADER, false, null)));
    assertEquals(1, get("C").version());
  }

  /** Covers T5.8. */
  @TestTemplate
  void testConcurrentCycleAndStaleNoOp() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("A", "B", "C", "D");
    edit("A", TermChange.addBroader("B"));
    edit("C", TermChange.addBroader("D"));
    String old = get("A").revisionToken();
    CyclicBarrier start = new CyclicBarrier(2);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 20; round++) {
        List<Future<Boolean>> results = new ArrayList<>();
        for (String[] pair : List.of(new String[] {"B", "C"}, new String[] {"D", "A"})) {
          results.add(
              workers.submit(
                  () -> {
                    start.await(10, TimeUnit.SECONDS);
                    try {
                      edit(pair[0], TermChange.addBroader(pair[1]));
                      return true;
                    } catch (IllegalArgumentException cycle) {
                      return false;
                    }
                  }));
        }
        assertNotEquals(
            results.get(0).get(30, TimeUnit.SECONDS), results.get(1).get(30, TimeUnit.SECONDS));
        String source = results.get(0).get() ? "B" : "D";
        String target = source.equals("B") ? "C" : "A";
        assertEquals(
            List.of(target), names(manager.listRelations(LAKE, source, BROADER, false, null)));
        assertTrue(
            manager
                .listRelations(LAKE, source.equals("B") ? "D" : "B", BROADER, false, null)
                .isEmpty());
        edit(source, TermChange.removeBroader(target));
      }
    } finally {
      workers.shutdownNow();
    }
    edit("A", TermChange.updateDefinition("fresh"));
    assertThrows(
        TermVersionConflictException.class,
        () -> manager.alterTerm(LAKE, "A", old, TermChange.addBroader("B")));
    assertThrows(
        TermVersionConflictException.class,
        () -> manager.alterTerm(LAKE, "A", old, TermChange.addBroader("missing")));
  }

  @TestTemplate
  void testSnapshotWhileAnotherSessionChangesVisibilityAndEdges() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("root", "old", "new");
    edit("root", TermChange.addBroader("old"));
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      GlossaryManager snapshot =
          manager.withTermVisibility(
              term -> {
                if ("root".equals(term.name())) {
                  try {
                    writer
                        .submit(
                            () ->
                                edit(
                                    "root",
                                    TermChange.setStatus(GlossaryTerm.Status.DRAFT),
                                    TermChange.removeBroader("old"),
                                    TermChange.addBroader("new")))
                        .get(15, TimeUnit.SECONDS);
                  } catch (Exception e) {
                    throw new AssertionError(e);
                  }
                }
                return true;
              });
      assertEquals(
          List.of("old"),
          names(snapshot.listRelations(LAKE, "root", BROADER, true, GlossaryTerm.Status.APPROVED)));
      assertThrows(
          NoSuchTermException.class,
          () -> manager.listRelations(LAKE, "root", BROADER, true, GlossaryTerm.Status.APPROVED));
      assertEquals(List.of("new"), names(manager.listRelations(LAKE, "root", BROADER, true, null)));
    } finally {
      writer.shutdownNow();
    }
  }

  /** Covers T5.9 and T5.16. */
  @TestTemplate
  void testDeleteRacesMixedAlterAndCreateInBothOrders() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (boolean creating : List.of(false, true)) {
      for (boolean dropFirst : List.of(false, true)) {
        String suffix = "-" + creating + "-" + dropFirst;
        String target = "target" + suffix, source = "source" + suffix, renamed = "updated" + suffix;
        manager.createTerm(
            LAKE,
            TermSpec.builder(target)
                .withStatus(creating ? GlossaryTerm.Status.DRAFT : GlossaryTerm.Status.DEPRECATED)
                .build());
        if (!creating) create(source);
        CountDownLatch locked = new CountDownLatch(1),
            release = new CountDownLatch(1),
            started = new CountDownLatch(1);
        GlossaryManager holder =
            new GlossaryManager(
                ids::incrementAndGet,
                false,
                (before, after, persist) -> {
                  locked.countDown();
                  await(release);
                  persist.run();
                });
        Consumer<GlossaryManager> write =
            editor -> {
              if (creating)
                editor.createTerm(LAKE, TermSpec.builder(renamed).withBroader(target).build());
              else
                editor.alterTerm(
                    LAKE, source, TermChange.rename(renamed), TermChange.addBroader(target));
            };
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
          Future<?> first =
              pool.submit(
                  () -> {
                    if (dropFirst) assertTrue(holder.dropTerm(LAKE, target));
                    else write.accept(holder);
                  });
          await(locked);
          Future<?> second =
              pool.submit(
                  () -> {
                    started.countDown();
                    if (dropFirst)
                      assertThrows(NoSuchTermException.class, () -> write.accept(manager));
                    else assertTrue(manager.dropTerm(LAKE, target));
                  });
          await(started);
          assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
          release.countDown();
          first.get(20, TimeUnit.SECONDS);
          second.get(20, TimeUnit.SECONDS);
          if (dropFirst) {
            assertThrows(NoSuchTermException.class, () -> get(renamed));
            if (!creating) assertEquals(1, get(source).version());
          } else {
            assertTrue(manager.listRelations(LAKE, renamed, BROADER, false, null).isEmpty());
            assertEquals(creating ? 2 : 3, get(renamed).version());
          }
        } finally {
          release.countDown();
          pool.shutdownNow();
        }
      }
    }
  }

  @TestTemplate
  void testContentEditWaitsForConcurrentDrop() throws Exception {
    createAndInsertMakeLake(LAKE);
    create("target");
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch editing = new CountDownLatch(1);
    GlossaryManager dropper =
        new GlossaryManager(
            ids::incrementAndGet,
            false,
            (before, after, persist) -> {
              locked.countDown();
              await(release);
              persist.run();
            });
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> dropped = pool.submit(() -> dropper.dropTerm(LAKE, "target"));
      await(locked);
      Future<?> edit =
          pool.submit(
              () -> {
                editing.countDown();
                assertThrows(
                    NoSuchTermException.class,
                    () ->
                        manager.alterTerm(LAKE, "target", TermChange.updateDefinition("updated")));
              });
      await(editing);
      assertThrows(TimeoutException.class, () -> edit.get(100, TimeUnit.MILLISECONDS));
      release.countDown();
      assertTrue(dropped.get(20, TimeUnit.SECONDS));
      edit.get(20, TimeUnit.SECONDS);
      assertThrows(NoSuchTermException.class, () -> get("target"));
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }

  /** Covers T5.14. */
  @TestTemplate
  void testMixedRelationLoad() throws Exception {
    createAndInsertMakeLake(LAKE);
    for (int i = 0; i < 30; i++) create("term" + i);
    ExecutorService pool = Executors.newFixedThreadPool(8);
    List<Future<?>> writers = new ArrayList<>();
    CyclicBarrier start = new CyclicBarrier(8);
    try {
      for (int worker = 0; worker < 8; worker++) {
        int seed = worker;
        writers.add(
            pool.submit(
                () -> {
                  Random random = new Random(seed);
                  start.await();
                  for (int i = 0; i < 500; i++) {
                    String source = "term" + random.nextInt(30),
                        target = "term" + random.nextInt(30);
                    try {
                      switch (random.nextInt(7)) {
                        case 0 -> edit(source, TermChange.addBroader(target));
                        case 1 -> edit(source, TermChange.removeBroader(target));
                        case 2 -> edit(source, TermChange.addRelated(target));
                        case 3 -> edit(source, TermChange.removeRelated(target));
                        case 4 -> edit(source, TermChange.updateDefinition("update" + i));
                        case 5 -> manager.dropTerm(LAKE, source);
                        default -> create(source);
                      }
                    } catch (NoSuchTermException
                        | TermAlreadyExistsException
                        | IllegalArgumentException expected) {
                      // Other writers may retire/create the term; graph constraints can reject
                      // additions.
                    }
                  }
                  return null;
                }));
      }
      for (Future<?> writer : writers) writer.get(10, TimeUnit.MINUTES);
      for (GlossaryTermEntity term : manager.listTerms(LAKE)) {
        Set<String> ancestors =
            new HashSet<>(names(manager.listRelations(LAKE, term.name(), BROADER, true, null)));
        for (String parent :
            names(manager.listRelations(LAKE, term.name(), BROADER, false, null))) {
          assertFalse(
              names(manager.listRelations(LAKE, parent, BROADER, true, null))
                  .contains(term.name()));
        }
        for (String related : names(manager.listRelations(LAKE, term.name(), RELATED, false, null)))
          assertFalse(ancestors.contains(related));
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(15, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static List<String> names(List<GlossaryTermEntity> terms) {
    return terms.stream().map(GlossaryTermEntity::name).toList();
  }

  private void create(String... names) {
    for (String name : names) manager.createTerm(LAKE, TermSpec.builder(name).build());
  }

  private GlossaryTermEntity get(String name) {
    return manager.getTerm(LAKE, name);
  }

  private void edit(String name, TermChange... changes) {
    manager.alterTerm(LAKE, name, changes);
  }
}
