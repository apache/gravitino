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

package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.glossary.GlossaryTerm.Status;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Backend contract tests for persisted glossary definitions. */
public class TestGlossaryTermMetaService extends TestJDBCBackend {
  private static final Entity.EntityType TYPE = Entity.EntityType.GLOSSARY_TERM;
  private static final Namespace NS =
      Namespace.of("glossary", Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term");
  private final GlossaryTermMetaService service = GlossaryTermMetaService.getInstance();

  @AfterEach
  public void checkInvariants() throws Exception {
    GlossaryInvariants.assertDefinitions();
  }

  private void setupMetalake() throws Exception {
    backend.insert(createBaseMakeLake(100L, "glossary", AUDIT_INFO), false);
  }

  private GlossaryTermEntity.Builder term(long id, String name) {
    return GlossaryTermEntity.builder()
        .withId(id)
        .withNamespace(NS)
        .withName(name)
        .withAuditInfo(AUDIT_INFO);
  }

  private NameIdentifier ident(String name) {
    return NameIdentifier.of(NS, name);
  }

  @TestTemplate
  public void testRoundTripAndUpdate() throws Exception {
    setupMetalake();
    GlossaryTermEntity value =
        term(101, " Active Customer ")
            .withDefinition("Business definition")
            .withSynonyms(new String[] {"client", "buyer"})
            .withStatus(Status.DRAFT)
            .withLinks(new TermLink[] {TermLink.of("docs", "https://example.org/term")})
            .withSourceRef("import:101")
            .withProperties(Map.of("team", "sales"))
            .build();
    backend.insert(value, false);
    GlossaryTermEntity read = backend.get(ident("ACTIVE CUSTOMER"), TYPE);
    Assertions.assertEquals("Active Customer", read.name());
    Assertions.assertEquals(value.definition(), read.definition());
    Assertions.assertEquals(Status.DRAFT, read.status());
    Assertions.assertEquals(value.properties(), read.properties());
    Assertions.assertArrayEquals(value.links(), read.links());
    Assertions.assertEquals(value.sourceRef(), read.sourceRef());
    Assertions.assertEquals(AUDIT_INFO, read.auditInfo());
    Assertions.assertEquals(2, read.synonyms().length);
    Assertions.assertEquals(1, read.version());
    Assertions.assertArrayEquals(
        new long[] {100, 101},
        new RelationalEntityStoreIdResolver()
            .getEntityIds(ident("ACTIVE CUSTOMER"), TYPE)
            .fullIds());
    GlossaryTermEntity updated =
        service.update(
            ident("Active Customer"),
            old -> term(101, "Account").withSynonyms(new String[] {"customer"}).build());
    Assertions.assertEquals(2, updated.version());
    Assertions.assertFalse(backend.exists(ident("Active Customer"), TYPE));
    Assertions.assertArrayEquals(
        new String[] {"customer"}, service.get(ident("Account")).synonyms());
    Assertions.assertEquals(1, backend.list(NS, TYPE, true).size());
    Assertions.assertEquals(
        1, service.batchGet(Arrays.asList(ident("Account"), ident("absent"))).size());
  }

  @TestTemplate
  public void testNormalizedNamesAndUnicode() throws Exception {
    setupMetalake();
    backend.insert(term(101, "Churn").build(), false);
    for (String name : new String[] {" churn ", "CHURN"})
      Assertions.assertThrows(
          EntityAlreadyExistsException.class, () -> backend.insert(term(102, name).build(), false));
    backend.insert(term(103, "Active   Customer").build(), false);
    for (String name : new String[] {"active customer", "Active\tCustomer"})
      Assertions.assertThrows(
          EntityAlreadyExistsException.class, () -> backend.insert(term(104, name).build(), false));
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      Assertions.assertEquals("title", GlossaryTermNames.key("TITLE"));
    } finally {
      Locale.setDefault(previous);
    }
    String[] names = {"Cafe", "Café", "a/b.中文😀", "😀".repeat(128)};
    long id = 105;
    for (String name : names) {
      backend.insert(term(id++, name).build(), false);
      Assertions.assertEquals(name, service.get(ident(name)).name());
    }
  }

  @TestTemplate
  public void testNullableAndExactSourceReferences() throws Exception {
    setupMetalake();
    backend.insert(term(101, "One").build(), false);
    backend.insert(term(102, "Two").build(), false);
    backend.insert(term(103, "Three").withSourceRef("import:One").build(), false);
    backend.insert(term(104, "Four").withSourceRef("import:one").build(), false);
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> backend.insert(term(105, "Five").withSourceRef("import:One").build(), false));
    Assertions.assertFalse(backend.exists(ident("Five"), TYPE));
  }

  @TestTemplate
  public void testExactSourceReferenceAndUnicodeBound() throws Exception {
    setupMetalake();
    backend.insert(term(101, "One").withSourceRef("import:a").build(), false);
    backend.insert(term(102, "Two").withSourceRef("import:a ").build(), false);
    backend.insert(term(103, "Three").withSourceRef("😀".repeat(128)).build(), false);
    Assertions.assertEquals("import:a ", service.get(ident("Two")).sourceRef());
    Assertions.assertEquals("😀".repeat(128), service.get(ident("Three")).sourceRef());
  }

  @TestTemplate
  public void testOuterTransactionRollsBackDefinitionAndSynonyms() throws Exception {
    setupMetalake();
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> {
                  try {
                    service.insert(
                        term(101, "One").withSynonyms(new String[] {"first"}).build(), false);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                  throw new IllegalStateException("abort outer transaction");
                }));
    Assertions.assertFalse(backend.exists(ident("One"), TYPE));
    Assertions.assertEquals(0, service.list(NS).size());
    backend.insert(term(101, "One").build(), false);
    Assertions.assertEquals(0, service.get(ident("One")).synonyms().length);
  }

  @TestTemplate
  public void testSynonymsAndRollback() throws Exception {
    setupMetalake();
    backend.insert(term(101, "One").withSynonyms(new String[] {"shared"}).build(), false);
    backend.insert(term(102, "Two").withSynonyms(new String[] {"shared"}).build(), false);
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            service.update(
                ident("One"),
                old -> term(101, "Two").withSynonyms(new String[] {"replacement"}).build()));
    Assertions.assertArrayEquals(new String[] {"shared"}, service.get(ident("One")).synonyms());
    Assertions.assertEquals(1, service.get(ident("One")).version());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> service.update(ident("One"), old -> term(999, "One").build()));
  }

  @TestTemplate
  public void testDeleteRecreateAndGc() throws Exception {
    setupMetalake();
    backend.insert(term(101, "Churn").withSynonyms(new String[] {"loss"}).build(), false);
    Assertions.assertTrue(backend.delete(ident("Churn"), TYPE, false));
    Assertions.assertFalse(backend.delete(ident("Churn"), TYPE, false));
    backend.insert(term(102, "Churn").build(), false);
    Assertions.assertEquals(102, service.get(ident("Churn")).id());
    Assertions.assertEquals(0, service.collect(1, 10));
    Assertions.assertEquals(1, service.collect(Long.MAX_VALUE, 10));
    Assertions.assertEquals(102, service.get(ident("Churn")).id());
  }

  @TestTemplate
  public void testWrongNamespaceCannotAccessTerm() throws Exception {
    setupMetalake();
    backend.insert(term(101, "Churn").withSynonyms(new String[] {"loss"}).build(), false);
    for (Namespace wrong :
        new Namespace[] {
          Namespace.empty(),
          Namespace.of("glossary"),
          Namespace.of("glossary", "system"),
          Namespace.of("glossary", "catalog", "schema"),
          Namespace.of("glossary", "system", "schema"),
          Namespace.of("glossary", "catalog", "glossary_term"),
          Namespace.of("glossary", "system", "glossary_term", "extra")
        }) {
      NameIdentifier invalid = NameIdentifier.of(wrong, "Churn");
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> backend.delete(invalid, TYPE, false));
      Assertions.assertThrows(IllegalArgumentException.class, () -> backend.get(invalid, TYPE));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> backend.list(wrong, TYPE, true));
      Assertions.assertThrows(IllegalArgumentException.class, () -> service.getIds(invalid));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> service.batchGet(Arrays.asList(invalid)));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              service.update(
                  invalid,
                  old -> {
                    Assertions.fail(
                        "Invalid namespace must be rejected before calling the updater");
                    return old;
                  }));
      GlossaryTermEntity read = service.get(ident("Churn"));
      Assertions.assertEquals(101, read.id());
      Assertions.assertEquals(1, read.version());
      Assertions.assertArrayEquals(new String[] {"loss"}, read.synonyms());
    }
  }

  @TestTemplate
  public void testSameTimestampDeletionAndCollection() throws Exception {
    setupMetalake();
    long deletedAt = 123456L;
    // Cover name reuse and independent source-reference reuse, including metalake cleanup.
    for (int generation = 0; generation < 3; generation++) {
      long id = 101L + generation;
      String name = generation == 2 ? "Other name" : "Churn";
      backend.insert(
          term(id, name).withSourceRef("import:churn").withSynonyms(new String[] {"loss"}).build(),
          false);
      final boolean dropMetalake = generation == 2;
      SessionUtils.doWithCommit(
          GlossaryTermMetaMapper.class,
          mapper -> {
            if (dropMetalake) {
              mapper.dropMetalake(100L, deletedAt);
              mapper.dropMetalakeSynonyms(100L, deletedAt);
            } else {
              Assertions.assertEquals(1, mapper.delete(id, 1, deletedAt));
              mapper.deleteSynonyms(id, deletedAt);
            }
          });
      Assertions.assertFalse(backend.exists(ident(name), TYPE));
    }
    backend.insert(
        term(104, "Churn")
            .withSourceRef("import:churn")
            .withSynonyms(new String[] {"live synonym"})
            .build(),
        false);
    Assertions.assertEquals(0, service.collect(deletedAt, 10));
    Assertions.assertEquals(2, service.collect(deletedAt + 1, 2));
    Assertions.assertEquals(1, service.collect(deletedAt + 1, 2));
    Assertions.assertEquals(0, service.collect(Long.MAX_VALUE, 10));
    Assertions.assertEquals(104, service.get(ident("Churn")).id());
    Assertions.assertArrayEquals(
        new String[] {"live synonym"}, service.get(ident("Churn")).synonyms());
  }

  @TestTemplate
  public void testMetalakeCleanupIsolation() throws Exception {
    setupMetalake();
    backend.insert(term(101, "One").withSynonyms(new String[] {"first"}).build(), false);
    backend.insert(createBaseMakeLake(200L, "other", AUDIT_INFO), false);
    Namespace other = Namespace.of("other", Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term");
    backend.insert(term(201, "Other").withNamespace(other).build(), false);
    backend.delete(NameIdentifier.of("glossary"), Entity.EntityType.METALAKE, true);
    Assertions.assertEquals(1, service.list(other).size());
    Assertions.assertEquals(1, service.collect(Long.MAX_VALUE, 10));
  }

  @TestTemplate
  public void testOverwritePreservesIdentity() throws Exception {
    setupMetalake();
    backend.insert(term(101, "One").withSynonyms(new String[] {"first"}).build(), false);
    backend.insert(term(102, "ONE").withSynonyms(new String[] {"second"}).build(), true);
    GlossaryTermEntity read = service.get(ident("one"));
    Assertions.assertEquals(101, read.id());
    Assertions.assertEquals(2, read.version());
    Assertions.assertArrayEquals(new String[] {"second"}, read.synonyms());
  }
}
