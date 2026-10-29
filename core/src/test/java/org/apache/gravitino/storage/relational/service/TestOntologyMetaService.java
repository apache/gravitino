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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.ConceptType;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Tests ontology persistence, versioning and status handling through the relational service. */
public class TestOntologyMetaService extends TestJDBCBackend {

  private static final AuditInfo AUDIT =
      AuditInfo.builder()
          .withCreator("creator")
          .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
          .build();

  private final String metalakeName = GravitinoITUtils.genRandomName("tst_ontology_metalake");

  private Namespace namespace;

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    namespace = Namespace.of(metalakeName);
  }

  @TestTemplate
  public void testInsertGetListAndIdLookup() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("finance");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");

    OntologyMetaService.getInstance().insertOntology(original, false);

    assertEquals(
        original,
        OntologyMetaService.getInstance().getOntologyByIdentifier(original.nameIdentifier()));
    assertEquals(
        original.id(),
        OntologyMetaService.getInstance()
            .getOntologyIdByMetalakeIdAndName(metalakeId(), ontologyName));

    List<OntologyEntity> listed =
        OntologyMetaService.getInstance().listOntologiesByNamespace(namespace);
    assertEquals(1, listed.size());
    assertEquals(original, listed.get(0));

    assertEquals(1L, identityVersions(original.id())[0]);
    assertEquals(1L, identityVersions(original.id())[1]);
    assertEquals(Ontology.Status.DRAFT.name(), status(original.id()));

    Map<Integer, VersionState> versions = listVersions(original.id());
    assertEquals(1, versions.size());
    assertEquals(1, versions.get(1).version);
    assertEquals(ontologyName, versions.get(1).name);
    assertEquals("v1 comment", versions.get(1).comment);
    assertTrue(versions.get(1).concepts.contains("Account"));
    assertTrue(versions.get(1).aiContext.contains("AI context for v1"));
    assertTrue(versions.get(1).requires.contains("Account IS NOT NULL"));
    assertTrue(versions.get(1).prefixes.contains("http://example.com/v1/"));
    assertTrue(versions.get(1).properties.contains("\"marker\":\"v1\""));
    assertEquals(0L, versions.get(1).deletedAt);
  }

  @TestTemplate
  public void testInsertRejectsDuplicateWithoutOverwrite() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("duplicate");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity duplicate =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v2 comment", "Party", "v2");
    assertThrows(
        EntityAlreadyExistsException.class,
        () -> OntologyMetaService.getInstance().insertOntology(duplicate, false));
    assertEquals(1, listVersions(original.id()).size());
  }

  @TestTemplate
  public void testOverwriteAllocatesNextVersion() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("overwrite");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity overwritten = ontology(original.id(), ontologyName, "v2 comment", "Party", "v2");
    OntologyMetaService.getInstance().insertOntology(overwritten, true);

    OntologyEntity loaded =
        OntologyMetaService.getInstance().getOntologyByIdentifier(original.nameIdentifier());
    assertEquals(original.id(), loaded.id());
    assertEquals("v2 comment", loaded.comment());
    assertEquals(2L, identityVersions(original.id())[0]);

    Map<Integer, VersionState> versions = listVersions(original.id());
    assertEquals(2, versions.size());
    assertEquals("v1 comment", versions.get(1).comment);
    assertTrue(versions.get(1).concepts.contains("Account"));
    assertEquals("v2 comment", versions.get(2).comment);
    assertTrue(versions.get(2).concepts.contains("Party"));
  }

  @TestTemplate
  public void testContentUpdateAllocatesNewVersion() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("content_update");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity next = ontology(original.id(), ontologyName, "v2 comment", "Party", "v2");
    OntologyEntity updated =
        OntologyMetaService.getInstance()
            .updateOntology(original.nameIdentifier(), ignored -> next);

    assertEquals(next, updated);
    assertEquals(
        next, OntologyMetaService.getInstance().getOntologyByIdentifier(original.nameIdentifier()));
    assertEquals(2L, identityVersions(original.id())[0]);
    assertEquals(2L, identityVersions(original.id())[1]);

    Map<Integer, VersionState> versions = listVersions(original.id());
    assertEquals(2, versions.size());
    assertEquals(0L, versions.get(1).deletedAt);
    assertEquals(0L, versions.get(2).deletedAt);
  }

  @TestTemplate
  public void testRenameKeepsTheWholeSnapshot() throws IOException {
    String oldName = GravitinoITUtils.genRandomName("rename_old");
    String newName = GravitinoITUtils.genRandomName("rename_new");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), oldName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity renamed = ontology(original.id(), newName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().updateOntology(original.nameIdentifier(), ignored -> renamed);

    assertThrows(
        NoSuchEntityException.class,
        () -> OntologyMetaService.getInstance().getOntologyByIdentifier(original.nameIdentifier()));
    assertEquals(
        renamed,
        OntologyMetaService.getInstance().getOntologyByIdentifier(renamed.nameIdentifier()));

    Map<Integer, VersionState> versions = listVersions(original.id());
    assertEquals(oldName, versions.get(1).name);
    assertEquals(newName, versions.get(2).name);
  }

  @TestTemplate
  public void testStatusFlipDoesNotAllocateVersion() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("status_flip");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);
    assertEquals(Ontology.Status.DRAFT.name(), status(original.id()));

    OntologyEntity published =
        OntologyMetaService.getInstance()
            .updateOntologyStatus(original.nameIdentifier(), Ontology.Status.PUBLISHED);

    assertEquals(Ontology.Status.PUBLISHED, published.status());
    assertEquals(Ontology.Status.PUBLISHED.name(), status(original.id()));
    assertEquals(1L, identityVersions(original.id())[0]);
    assertEquals(1L, identityVersions(original.id())[1]);
    assertEquals(1, listVersions(original.id()).size());

    OntologyEntity republished =
        OntologyMetaService.getInstance()
            .updateOntologyStatus(original.nameIdentifier(), Ontology.Status.DRAFT);
    assertEquals(Ontology.Status.DRAFT, republished.status());
    assertEquals(1L, identityVersions(original.id())[0]);
    assertEquals(1, listVersions(original.id()).size());
  }

  @TestTemplate
  public void testStatusOnlyUpdateThroughUpdaterDoesNotAllocateVersion() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("status_updater");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity published =
        OntologyEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(original.namespace())
            .withComment(original.comment())
            .withStatus(Ontology.Status.PUBLISHED)
            .withDefinition(original.definition())
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .build();

    OntologyEntity updated =
        OntologyMetaService.getInstance()
            .updateOntology(original.nameIdentifier(), ignored -> published);

    assertEquals(Ontology.Status.PUBLISHED, updated.status());
    assertEquals(Ontology.Status.PUBLISHED.name(), status(original.id()));
    assertEquals(1L, identityVersions(original.id())[0]);
    assertEquals(1, listVersions(original.id()).size());
  }

  @TestTemplate
  public void testContentChangeWithStatusChangeStillAllocatesVersion() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("status_and_content");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity next =
        OntologyEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(original.namespace())
            .withComment("v2 comment")
            .withStatus(Ontology.Status.PUBLISHED)
            .withDefinition(original.definition())
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .build();

    OntologyMetaService.getInstance().updateOntology(original.nameIdentifier(), ignored -> next);

    assertEquals(Ontology.Status.PUBLISHED.name(), status(original.id()));
    assertEquals(2L, identityVersions(original.id())[0]);
    assertEquals(2, listVersions(original.id()).size());
  }

  @TestTemplate
  public void testUpdateRejectsIdAndNamespaceChange() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("identity_guard");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    OntologyEntity otherId =
        OntologyEntity.builder()
            .withId(original.id() + 1000)
            .withName(original.name())
            .withNamespace(original.namespace())
            .withComment(original.comment())
            .withStatus(original.status())
            .withDefinition(original.definition())
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyMetaService.getInstance()
                .updateOntology(original.nameIdentifier(), ignored -> otherId));

    OntologyEntity otherNamespace =
        OntologyEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(Namespace.of("another_metalake"))
            .withComment(original.comment())
            .withStatus(original.status())
            .withDefinition(original.definition())
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyMetaService.getInstance()
                .updateOntology(original.nameIdentifier(), ignored -> otherNamespace));
  }

  @TestTemplate
  public void testDeleteSoftDeletesOntologyAndVersions() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("delete");
    OntologyEntity original =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(original, false);

    assertTrue(OntologyMetaService.getInstance().deleteOntology(original.nameIdentifier()));

    assertThrows(
        NoSuchEntityException.class,
        () -> OntologyMetaService.getInstance().getOntologyByIdentifier(original.nameIdentifier()));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OntologyMetaService.getInstance()
                .getOntologyIdByMetalakeIdAndName(metalakeId(), ontologyName));
    assertTrue(listVersions(original.id()).get(1).deletedAt > 0L);
    assertTrue(OntologyMetaService.getInstance().listOntologiesByNamespace(namespace).isEmpty());

    assertFalse(OntologyMetaService.getInstance().deleteOntology(original.nameIdentifier()));
  }

  @TestTemplate
  public void testGetMissingOntologyAndMetalake() throws IOException {
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OntologyMetaService.getInstance()
                .getOntologyByIdentifier(NameIdentifier.of(metalakeName, "missing")));
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OntologyMetaService.getInstance()
                .listOntologiesByNamespace(Namespace.of("missing_metalake")));
    assertFalse(
        OntologyMetaService.getInstance()
            .deleteOntology(NameIdentifier.of(metalakeName, "missing")));
  }

  @TestTemplate
  public void testNamespaceMustHaveOneLevel() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyMetaService.getInstance()
                .listOntologiesByNamespace(Namespace.of(metalakeName, "catalog")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyMetaService.getInstance()
                .getOntologyByIdentifier(NameIdentifier.of(metalakeName, "catalog", "ontology")));
  }

  @TestTemplate
  public void testSoftDropVersionRetentionAndLegacyGarbageCollection() throws IOException {
    String ontologyName = GravitinoITUtils.genRandomName("retention");
    OntologyEntity current =
        ontology(RandomIdGenerator.INSTANCE.nextId(), ontologyName, "v1 comment", "Account", "v1");
    OntologyMetaService.getInstance().insertOntology(current, false);
    for (int version = 2; version <= 3; version++) {
      OntologyEntity next =
          ontology(current.id(), ontologyName, "v" + version + " comment", "Party", "v" + version);
      current =
          OntologyMetaService.getInstance()
              .updateOntology(current.nameIdentifier(), ignored -> next);
    }

    assertEquals(
        2, OntologyMetaService.getInstance().deleteOntologyVersionsByRetentionCount(1L, 100));
    Map<Integer, VersionState> retained = listVersions(current.id());
    assertTrue(retained.get(1).deletedAt > 0L, () -> "retained versions: " + retained.keySet());
    assertTrue(retained.get(2).deletedAt > 0L, () -> "retained versions: " + retained.keySet());
    assertEquals(0L, retained.get(3).deletedAt);

    OntologyEntity finalCurrent = current;
    assertTrue(OntologyMetaService.getInstance().deleteOntology(finalCurrent.nameIdentifier()));
    assertTrue(listVersions(finalCurrent.id()).get(3).deletedAt > 0L);

    int deleted =
        OntologyMetaService.getInstance()
            .deleteOntologyMetasByLegacyTimeline(Instant.now().toEpochMilli() + 1000, 100);
    assertEquals(4, deleted);
    assertEquals(0, listVersions(finalCurrent.id()).size());
    assertEquals(0, countIdentities(finalCurrent.id()));
  }

  private Long metalakeId() {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);
  }

  private OntologyEntity ontology(
      Long id, String name, String comment, String concept, String marker) {
    return OntologyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withStatus(Ontology.Status.DRAFT)
        .withDefinition(definition(concept, marker))
        .withProperties(Map.of("marker", marker))
        .withAuditInfo(AUDIT)
        .build();
  }

  private static OntologyDefinition definition(String concept, String marker) {
    return OntologyDefinition.builder()
        .withAiContext(AIContext.of("AI context for " + marker))
        .withRequires("Account IS NOT NULL")
        .withConcepts(
            OntologyConcept.builder()
                .withConcept(concept)
                .withType(ConceptType.ENTITY_TYPE)
                .build(),
            OntologyConcept.builder()
                .withConcept(marker + "_value")
                .withType(ConceptType.VALUE_TYPE)
                .build())
        .withPrefixes(Map.of("ex", "http://example.com/" + marker + "/"))
        .build();
  }

  private Map<Integer, VersionState> listVersions(Long ontologyId) {
    Map<Integer, VersionState> versions = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT version, ontology_name, ontology_comment, ontology, ai_context,"
                        + " requires_sql, prefixes, properties, deleted_at"
                        + " FROM ontology_version_info WHERE ontology_id = %d",
                    ontologyId))) {
      while (resultSet.next()) {
        int version = resultSet.getInt("version");
        versions.put(
            version,
            new VersionState(
                version,
                resultSet.getString("ontology_name"),
                resultSet.getString("ontology_comment"),
                resultSet.getString("ontology"),
                resultSet.getString("ai_context"),
                resultSet.getString("requires_sql"),
                resultSet.getString("prefixes"),
                resultSet.getString("properties"),
                resultSet.getLong("deleted_at")));
      }
      return versions;
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private long[] identityVersions(Long ontologyId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT current_version, last_version FROM ontology_meta"
                        + " WHERE ontology_id = %d",
                    ontologyId))) {
      assertTrue(resultSet.next());
      return new long[] {resultSet.getLong("current_version"), resultSet.getLong("last_version")};
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private String status(Long ontologyId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT status FROM ontology_meta WHERE ontology_id = %d", ontologyId))) {
      assertTrue(resultSet.next());
      return resultSet.getString("status");
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private int countIdentities(Long ontologyId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format(
                    "SELECT COUNT(*) FROM ontology_meta WHERE ontology_id = %d", ontologyId))) {
      assertTrue(resultSet.next());
      return resultSet.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private static final class VersionState {
    private final int version;
    private final String name;
    private final String comment;
    private final String concepts;
    @Nullable private final String aiContext;
    @Nullable private final String requires;
    @Nullable private final String prefixes;
    @Nullable private final String properties;
    private final long deletedAt;

    private VersionState(
        int version,
        String name,
        @Nullable String comment,
        String concepts,
        @Nullable String aiContext,
        @Nullable String requires,
        @Nullable String prefixes,
        @Nullable String properties,
        long deletedAt) {
      assertNotNull(concepts);
      this.version = version;
      this.name = name;
      this.comment = comment;
      this.concepts = concepts;
      this.aiContext = aiContext;
      this.requires = requires;
      this.prefixes = prefixes;
      this.properties = properties;
      this.deletedAt = deletedAt;
    }
  }
}
