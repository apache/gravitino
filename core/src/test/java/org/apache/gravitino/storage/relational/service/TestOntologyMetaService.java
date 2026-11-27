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

import static org.apache.gravitino.ontology.OssieOntologyDocuments.DEFAULT_VERSION;
import static org.apache.gravitino.ontology.OssieOntologyDocuments.VERSION_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.exceptions.OntologyMappingAlreadyExistsException;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.OntologyMappingDefinition;
import org.apache.gravitino.ontology.OssieOntologyDocuments;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

/** Exercises the versioned ontology store on Gravitino's relational backends. */
public class TestOntologyMetaService extends TestJDBCBackend {
  private static final String METALAKE = "ontology_tests";
  private static final String CATALOG = "sales";
  private static final String SCHEMA = "reporting";
  private static final String ONTOLOGY = "Sales";
  private static final String MAPPING = "sales_mapping";
  private static final NameIdentifier MODEL =
      NameIdentifier.of(METALAKE, CATALOG, SCHEMA, "sales_model");
  private static final OntologyMetaService STORE = OntologyMetaService.getInstance();
  private static final OntologyMappingDefinition MAPPING_DEFINITION =
      mappingDefinition("customer.id");

  /** Creates the parent namespace and initial model. */
  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, CATALOG);
    createAndInsertSchema(METALAKE, CATALOG, SCHEMA);
    insertModel(MODEL, SemanticModel.Status.PUBLISHED);
  }

  /** Checks ID-based references, compatibility rollback, and both kinds of dropping. */
  @TestTemplate
  public void testLifecycleAndReferenceGuards() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            STORE.createOntology(
                RandomIdGenerator.INSTANCE,
                METALAKE,
                "*",
                null,
                definition("Customer"),
                Map.of(),
                ignored -> {}));
    OntologyEntity original = createOntology();
    assertThrows(OntologyAlreadyExistsException.class, this::createOntology);
    assertThrows(
        IllegalOntologyMappingException.class,
        () ->
            STORE.createMapping(
                RandomIdGenerator.INSTANCE,
                MODEL,
                MAPPING,
                null,
                "Missing",
                MAPPING_DEFINITION,
                Map.of(),
                ignored -> {}));
    assertThrows(
        ForbiddenException.class,
        () ->
            STORE.createMapping(
                RandomIdGenerator.INSTANCE,
                MODEL,
                MAPPING,
                null,
                ONTOLOGY,
                MAPPING_DEFINITION,
                Map.of(),
                ignored -> {
                  throw new ForbiddenException("Forbidden");
                }));
    assertTrue(STORE.listModelMappings(MODEL, ignored -> true).isEmpty());

    createMapping();
    assertEquals(List.of(MAPPING), STORE.listModelMappings(MODEL, ignored -> true));
    assertEquals(
        List.of(NameIdentifier.of(METALAKE, CATALOG, SCHEMA, MODEL.name(), MAPPING)),
        STORE.listOntologyMappings(METALAKE, ONTOLOGY, (ignoredMapping, published) -> true));

    OntologyInUseException visible =
        assertThrows(
            OntologyInUseException.class,
            () ->
                STORE.alterOntology(
                    METALAKE,
                    ONTOLOGY,
                    (mapping, published) -> true,
                    OntologyChange.rename("Rejected"),
                    OntologyChange.replaceDefinition(definition("Other"))));
    assertTrue(visible.getMessage().contains(MAPPING));
    assertTrue(visible.getMessage().contains("$.conceptMappings[0].concept"));
    OntologyInUseException hidden =
        assertThrows(
            OntologyInUseException.class,
            () -> STORE.dropOntology(METALAKE, ONTOLOGY, (mapping, published) -> false));
    assertTrue(hidden.getMessage().contains("1 mapping the caller cannot load"));
    assertFalse(hidden.getMessage().contains(MAPPING));
    assertFalse(hidden.getMessage().contains(MODEL.name()));
    assertEquals(List.of(ONTOLOGY), STORE.listOntologies(METALAKE));

    OntologyEntity renamed =
        STORE.alterOntology(
            METALAKE, ONTOLOGY, (mapping, published) -> true, OntologyChange.rename("NewSales"));
    assertEquals(original.id(), renamed.id());
    assertEquals(0, STORE.retireVersions(false, 1, 100));
    assertEquals("NewSales", STORE.loadMapping(MODEL, MAPPING, ignored -> true).ontology());
    assertNotNull(renamed.auditInfo().lastModifiedTime());

    assertThrows(
        ForbiddenException.class,
        () ->
            STORE.alterMapping(
                MODEL,
                MAPPING,
                ignored -> {
                  throw new ForbiddenException("Forbidden");
                },
                OntologyMappingChange.replaceDefinition(mappingDefinition("customer.key"))));
    OntologyMapping altered =
        STORE.alterMapping(
            MODEL,
            MAPPING,
            ignored -> {
              throw new AssertionError("No definition change");
            },
            OntologyMappingChange.updateComment("Current"),
            OntologyMappingChange.rename("new_mapping"));
    assertEquals("new_mapping", altered.name());
    assertEquals("Current", altered.comment());
    assertEquals(0, STORE.retireVersions(true, 10, 100));
    assertEquals(1, STORE.retireVersions(true, 1, 100));
    assertTrue(SemanticModelMetaService.getInstance().deleteSemanticModel(MODEL));
    assertTrue(
        STORE
            .listOntologyMappings(METALAKE, renamed.name(), (ignoredMapping, published) -> true)
            .isEmpty());
    assertTrue(STORE.dropOntology(METALAKE, renamed.name(), (mapping, published) -> true));
    assertFalse(STORE.dropOntology(METALAKE, renamed.name(), (mapping, published) -> true));
    assertEquals(3, STORE.purge(true, System.currentTimeMillis() + 1000, 100));
  }

  /** Verifies that only one live mapping can use a given ontology under the same model. */
  @TestTemplate
  public void testOneLiveMappingPerModelAndOntology() {
    createOntology();
    createMapping();
    OntologyMappingAlreadyExistsException duplicate =
        assertThrows(
            OntologyMappingAlreadyExistsException.class, () -> createMapping("second_mapping"));
    assertTrue(duplicate.getMessage().contains(ONTOLOGY));
    assertEquals(List.of(MAPPING), STORE.listModelMappings(MODEL, ignored -> true));

    assertTrue(STORE.dropMapping(MODEL, MAPPING));
    createMapping("second_mapping");
    assertEquals(List.of("second_mapping"), STORE.listModelMappings(MODEL, ignored -> true));
  }

  /** Verifies that a live expression mapping requires a single value-typed identifier. */
  @TestTemplate
  public void testIdentifierChangeRejectsLiveMapping() {
    String ontology =
        """
        version: 0.3.0.dev0
        name: Sales
        ontology:
          - concept: Customer
            type: EntityType
            identify_by: [%s]
            relationships:
              - name: id
                roles: [{concept: %s}]
                verbalizes: [id]
              - name: email
                roles: [{concept: String}]
                verbalizes: [email]
          - concept: Account
            type: EntityType
            identify_by: [number]
            relationships:
              - name: number
                roles: [{concept: Integer}]
                verbalizes: [number]
        """;
    OntologyDefinition before =
        OssieOntologyDocuments.importOntology(
                OssieDocument.yaml(ontology.formatted("id", "Integer")))
            .definition();
    STORE.createOntology(
        RandomIdGenerator.INSTANCE, METALAKE, ONTOLOGY, null, before, Map.of(), ignored -> {});
    createMapping();

    for (String incompatible :
        List.of(ontology.formatted("id, email", "Integer"), ontology.formatted("id", "Account"))) {
      OntologyDefinition replacement =
          OssieOntologyDocuments.importOntology(OssieDocument.yaml(incompatible)).definition();
      OntologyInUseException conflict =
          assertThrows(
              OntologyInUseException.class,
              () ->
                  STORE.alterOntology(
                      METALAKE,
                      ONTOLOGY,
                      (mapping, published) -> true,
                      OntologyChange.replaceDefinition(replacement)));
      assertTrue(
          conflict.getMessage().contains("$.conceptMappings[0].objectMappings[0].expression"));
      assertEquals(
          before, STORE.getEntity(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY)).definition());
      assertEquals(
          MAPPING_DEFINITION, STORE.loadMapping(MODEL, MAPPING, ignored -> true).definition());
    }
    assertEquals(0, STORE.retireVersions(false, 1, 100));
  }

  /** Verifies that reverse lookup and conflict reporting use the logical schema name. */
  @TestTemplate
  public void testMappingIdentifiersUseLogicalSchemaNames() throws SQLException {
    createOntology();
    createMapping();
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    String logical = String.join(separator, SCHEMA, "regional");
    String physical = HierarchicalSchemaUtil.logicalToPhysical(logical, separator);

    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        PreparedStatement rename =
            session
                .getConnection()
                .prepareStatement("UPDATE schema_meta SET schema_name = ? WHERE schema_name = ?")) {
      rename.setString(1, physical);
      rename.setString(2, SCHEMA);
      assertEquals(1, rename.executeUpdate());
      session.commit(true);
    }

    NameIdentifier expected = NameIdentifier.of(METALAKE, CATALOG, logical, MODEL.name(), MAPPING);
    assertEquals(
        List.of(expected),
        STORE.listOntologyMappings(METALAKE, ONTOLOGY, (ignoredMapping, published) -> true));
    OntologyInUseException conflict =
        assertThrows(
            OntologyInUseException.class,
            () ->
                STORE.dropOntology(
                    METALAKE, ONTOLOGY, (mapping, published) -> expected.equals(mapping)));
    assertTrue(
        conflict
            .getMessage()
            .contains(NameIdentifier.of(CATALOG, logical, MODEL.name(), MAPPING).toString()));
    assertFalse(conflict.getMessage().contains(physical));
  }

  /**
   * Checks that snapshots retire under the configured cutoff and model cascades remove mappings.
   */
  @TestTemplate
  public void testRetentionAndSchemaCascade() {
    createOntology();
    createMapping();
    OntologyEntity updated =
        STORE.alterOntology(
            METALAKE,
            ONTOLOGY,
            (mapping, published) -> true,
            OntologyChange.updateComment("second"),
            OntologyChange.setProperty("owner", "team"));
    STORE.alterOntology(
        METALAKE, ONTOLOGY, (mapping, published) -> true, OntologyChange.updateComment("third"));
    assertEquals(0, STORE.retireVersions(false, 10, 100));
    assertEquals(2, STORE.retireVersions(false, 1, 100));
    assertEquals("team", updated.properties().get("owner"));
    assertEquals(
        "third", STORE.getEntity(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY)).comment());
    assertTrue(
        SchemaMetaService.getInstance()
            .deleteSchema(NameIdentifier.of(METALAKE, CATALOG, SCHEMA), true));
    assertTrue(
        STORE
            .listOntologyMappings(METALAKE, ONTOLOGY, (ignoredMapping, published) -> true)
            .isEmpty());
    assertTrue(STORE.dropOntology(METALAKE, ONTOLOGY, (mapping, published) -> true));
  }

  /** Checks the catalog and metalake cascade paths. */
  @TestTemplate
  public void testCatalogAndMetalakeCascades() {
    OntologyEntity ontology = createOntology();
    createMapping();
    assertTrue(
        CatalogMetaService.getInstance().deleteCatalog(NameIdentifier.of(METALAKE, CATALOG), true));
    assertTrue(
        STORE
            .listOntologyMappings(METALAKE, ONTOLOGY, (ignoredMapping, published) -> true)
            .isEmpty());
    assertEquals(2, STORE.purge(true, System.currentTimeMillis() + 1000, 100));
    assertTrue(MetalakeMetaService.getInstance().deleteMetalake(NameIdentifier.of(METALAKE), true));
    assertTrue(STORE.namesById(List.of(ontology.id())).isEmpty());
    assertEquals(2, STORE.purge(false, System.currentTimeMillis() + 1000, 100));
  }

  /** Verifies that overlapping writes cannot leave a reference to a removed concept. */
  @TestTemplate
  public void testConcurrentMappingCreateAndOntologyReplace() throws Exception {
    createOntology();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService threads = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> mappingCreated =
          threads.submit(
              () -> {
                assertTrue(start.await(10, TimeUnit.SECONDS));
                try {
                  createMapping();
                  return true;
                } catch (IllegalOntologyMappingException expected) {
                  return false;
                }
              });
      Future<Boolean> ontologyReplaced =
          threads.submit(
              () -> {
                assertTrue(start.await(10, TimeUnit.SECONDS));
                try {
                  STORE.alterOntology(
                      METALAKE,
                      ONTOLOGY,
                      (mapping, published) -> true,
                      OntologyChange.replaceDefinition(definition("Other")));
                  return true;
                } catch (OntologyInUseException expected) {
                  return false;
                }
              });
      start.countDown();
      boolean mappingWon = mappingCreated.get(30, TimeUnit.SECONDS);
      assertEquals(!mappingWon, ontologyReplaced.get(30, TimeUnit.SECONDS));
      if (mappingWon) {
        assertEquals(
            MAPPING_DEFINITION, STORE.loadMapping(MODEL, MAPPING, ignored -> true).definition());
      } else {
        assertThrows(
            NoSuchOntologyMappingException.class,
            () -> STORE.loadMapping(MODEL, MAPPING, ignored -> true));
      }
    } finally {
      threads.shutdownNow();
    }
  }

  /** Verifies that repeating current values leaves the version history unchanged. */
  @TestTemplate
  public void testNoOpUpdatesDoNotCreateVersions() {
    createOntology();
    createMapping();
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        (mapping, published) -> true,
        OntologyChange.setProperty("owner", "team"),
        OntologyChange.updateComment("current"));
    OntologyEntity unchanged =
        STORE.alterOntology(
            METALAKE,
            ONTOLOGY,
            (mapping, published) -> true,
            OntologyChange.setProperty("owner", "team"),
            OntologyChange.updateComment("current"),
            OntologyChange.removeProperty("missing"),
            OntologyChange.replaceDefinition(definition("Customer")));
    assertEquals("current", unchanged.comment());
    assertEquals(1, STORE.retireVersions(false, 1, 100));

    STORE.alterMapping(
        MODEL, MAPPING, ignored -> {}, OntologyMappingChange.setProperty("owner", "team"));
    assertThrows(
        ForbiddenException.class,
        () ->
            STORE.alterMapping(
                MODEL,
                MAPPING,
                ignored -> {
                  throw new ForbiddenException("Forbidden");
                },
                OntologyMappingChange.setProperty("owner", "another_team"),
                OntologyMappingChange.replaceDefinition(MAPPING_DEFINITION)));
    OntologyMapping mapping =
        STORE.alterMapping(
            MODEL,
            MAPPING,
            ontology -> assertEquals(ONTOLOGY, ontology),
            OntologyMappingChange.setProperty("owner", "team"),
            OntologyMappingChange.replaceDefinition(MAPPING_DEFINITION));
    assertEquals("team", mapping.properties().get("owner"));
    assertEquals(1, STORE.retireVersions(true, 1, 100));
  }

  /** Checks mapping reads and conflict details before and after the model's first publication. */
  @TestTemplate
  public void testMappingVisibilityFollowsModelPublication() throws IOException {
    NameIdentifier model = NameIdentifier.of(MODEL.namespace(), "deferred_model");
    SemanticModelEntity draft = insertModel(model, SemanticModel.Status.DRAFT);
    createOntology();
    STORE.createMapping(
        RandomIdGenerator.INSTANCE,
        model,
        MAPPING,
        null,
        ONTOLOGY,
        MAPPING_DEFINITION,
        Map.of(),
        ignored -> {});

    BiPredicate<NameIdentifier, Boolean> useOnly =
        (mapping, modelHasPublishedRevision) -> modelHasPublishedRevision;
    NameIdentifier identifier = NameIdentifier.of(METALAKE, CATALOG, SCHEMA, model.name(), MAPPING);
    Predicate<NameIdentifier> cannotModify = ignored -> false;
    assertTrue(STORE.listOntologyMappings(METALAKE, ONTOLOGY, useOnly).isEmpty());
    assertEquals(
        List.of(identifier),
        STORE.listOntologyMappings(METALAKE, ONTOLOGY, (ignoredMapping, published) -> true));
    assertThrows(
        NoSuchSemanticModelException.class, () -> STORE.listModelMappings(model, cannotModify));
    assertThrows(
        NoSuchSemanticModelException.class, () -> STORE.loadMapping(model, MAPPING, cannotModify));
    assertEquals(List.of(MAPPING), STORE.listModelMappings(model, ignored -> true));
    assertEquals(MAPPING, STORE.loadMapping(model, MAPPING, ignored -> true).name());

    Executable drop = () -> STORE.dropOntology(METALAKE, ONTOLOGY, useOnly);
    Executable replace =
        () ->
            STORE.alterOntology(
                METALAKE, ONTOLOGY, useOnly, OntologyChange.replaceDefinition(definition("Other")));
    String hidden = "Ontology Sales is used by 1 mapping the caller cannot load";
    assertEquals(hidden, assertThrows(OntologyInUseException.class, drop).getMessage());
    assertEquals(hidden, assertThrows(OntologyInUseException.class, replace).getMessage());

    SemanticModelMetaService models = SemanticModelMetaService.getInstance();
    models.publishRevision(model, 1, "publisher");
    SemanticModelEntity nextDraft = models.updateSemanticModel(model, ignored -> draft);
    assertEquals(2, nextDraft.revision());
    assertEquals(SemanticModel.Status.DRAFT, nextDraft.status());
    assertEquals(List.of(identifier), STORE.listOntologyMappings(METALAKE, ONTOLOGY, useOnly));
    assertEquals(List.of(MAPPING), STORE.listModelMappings(model, cannotModify));
    assertEquals(MAPPING, STORE.loadMapping(model, MAPPING, cannotModify).name());
    assertTrue(assertThrows(OntologyInUseException.class, drop).getMessage().contains(MAPPING));
    assertTrue(
        assertThrows(OntologyInUseException.class, replace)
            .getMessage()
            .contains(MAPPING + " at "));
  }

  /** Verifies that conflict visibility is decided after the transaction releases its locks. */
  @TestTemplate
  public void testMappingVisibilityIsCheckedOutsideTransaction() {
    createOntology();
    createMapping();
    AtomicBoolean checked = new AtomicBoolean();
    AtomicBoolean checkedInTransaction = new AtomicBoolean();
    OntologyInUseException conflict =
        assertThrows(
            OntologyInUseException.class,
            () ->
                STORE.alterOntology(
                    METALAKE,
                    ONTOLOGY,
                    (mapping, published) -> {
                      checked.set(true);
                      checkedInTransaction.compareAndSet(false, SessionUtils.isInTransaction());
                      return true;
                    },
                    OntologyChange.replaceDefinition(definition("Other"))));
    assertTrue(conflict.getMessage().contains(MAPPING));
    assertThrows(
        OntologyInUseException.class,
        () ->
            STORE.dropOntology(
                METALAKE,
                ONTOLOGY,
                (mapping, published) -> {
                  checkedInTransaction.compareAndSet(false, SessionUtils.isInTransaction());
                  return false;
                }));
    assertTrue(checked.get());
    assertFalse(checkedInTransaction.get());
    assertEquals(
        definition("Customer").fields(),
        STORE.getEntity(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY)).definition().fields());
  }

  /** Covers rename conflicts and authorization order. */
  @TestTemplate
  public void testRenameConflictsAndAuthorizationOrder() {
    OntologyEntity sales = createOntology();
    STORE.createOntology(
        RandomIdGenerator.INSTANCE,
        METALAKE,
        "Other",
        null,
        definition("Customer"),
        Map.of(),
        ignored -> {});
    createMapping();
    STORE.createMapping(
        RandomIdGenerator.INSTANCE,
        MODEL,
        "other_mapping",
        null,
        "Other",
        MAPPING_DEFINITION,
        Map.of(),
        ignored -> {});

    assertEquals(sales.id(), STORE.getId(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY)));
    assertTrue(STORE.namesById(List.of()).isEmpty());
    assertThrows(
        NoSuchEntityException.class,
        () -> STORE.getId(NameIdentifierUtil.ofOntology(METALAKE, "Missing")));

    OntologyAlreadyExistsException ontologyConflict =
        assertThrows(
            OntologyAlreadyExistsException.class,
            () ->
                STORE.alterOntology(
                    METALAKE,
                    ONTOLOGY,
                    (mapping, published) -> true,
                    OntologyChange.rename("Other")));
    assertTrue(ontologyConflict.getMessage().contains("'Other'"));
    OntologyMappingAlreadyExistsException mappingConflict =
        assertThrows(
            OntologyMappingAlreadyExistsException.class,
            () ->
                STORE.alterMapping(
                    MODEL, MAPPING, ignored -> {}, OntologyMappingChange.rename("other_mapping")));
    assertTrue(mappingConflict.getMessage().contains("'other_mapping'"));

    // A caller who cannot load ontologies learns nothing about whether a name exists.
    assertThrows(
        ForbiddenException.class,
        () ->
            STORE.createMapping(
                RandomIdGenerator.INSTANCE,
                MODEL,
                "probe",
                null,
                "Missing",
                MAPPING_DEFINITION,
                Map.of(),
                ignored -> {
                  throw new ForbiddenException("Forbidden");
                }));
  }

  /** Verifies that removing the stored version makes both objects export the pin. */
  @TestTemplate
  public void testRemoveOssieVersionFallsBackOnExport() {
    createOntology();
    createMapping();
    String version = "0.3.0.dev9";
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        (mapping, published) -> true,
        OntologyChange.setProperty(VERSION_PROPERTY, version));
    STORE.alterMapping(
        MODEL,
        MAPPING,
        ignored -> {},
        OntologyMappingChange.setProperty(VERSION_PROPERTY, version));
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        (mapping, published) -> true,
        OntologyChange.removeProperty(VERSION_PROPERTY));
    STORE.alterMapping(
        MODEL, MAPPING, ignored -> {}, OntologyMappingChange.removeProperty(VERSION_PROPERTY));

    OntologyEntity ontology = STORE.getEntity(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY));
    OntologyMapping mapping = STORE.loadMapping(MODEL, MAPPING, ignored -> true);
    assertFalse(ontology.properties().containsKey(VERSION_PROPERTY));
    assertFalse(mapping.properties().containsKey(VERSION_PROPERTY));
    assertEquals(
        DEFAULT_VERSION,
        OssieOntologyDocuments.importOntology(
                OssieOntologyDocuments.exportOntology(ontology, OssieFormat.JSON))
            .properties()
            .get(VERSION_PROPERTY));
    assertEquals(
        DEFAULT_VERSION,
        OssieOntologyDocuments.importMapping(
                OssieOntologyDocuments.exportMapping(mapping, MODEL.name(), OssieFormat.YAML),
                MODEL.name())
            .properties()
            .get(VERSION_PROPERTY));
  }

  /** Verifies that reverting a replacement still requires authorization and creates no version. */
  @TestTemplate
  public void testRevertingBatchCreatesNoVersion() {
    createOntology();
    createMapping();
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        (mapping, published) -> {
          throw new AssertionError("The final definition is unchanged");
        },
        OntologyChange.setProperty("owner", "team"),
        OntologyChange.removeProperty("owner"),
        OntologyChange.replaceDefinition(definition("Other")),
        OntologyChange.replaceDefinition(definition("Customer")));
    OntologyMappingChange[] changes = {
      OntologyMappingChange.updateComment("temporary"),
      OntologyMappingChange.updateComment(null),
      OntologyMappingChange.replaceDefinition(mappingDefinition("customer.key")),
      OntologyMappingChange.replaceDefinition(MAPPING_DEFINITION)
    };
    assertThrows(
        ForbiddenException.class,
        () ->
            STORE.alterMapping(
                MODEL,
                MAPPING,
                ignored -> {
                  throw new ForbiddenException("Forbidden");
                },
                changes));
    STORE.alterMapping(MODEL, MAPPING, ontology -> assertEquals(ONTOLOGY, ontology), changes);
    assertEquals(0, STORE.retireVersions(false, 1, 100));
    assertEquals(0, STORE.retireVersions(true, 1, 100));
  }

  private SemanticModelEntity insertModel(NameIdentifier model, SemanticModel.Status status)
      throws IOException {
    Dataset dataset =
        Dataset.builder()
            .withName("customer")
            .withSource(NameIdentifier.of(CATALOG, SCHEMA, "customer"))
            .build();
    SemanticModelEntity entity =
        SemanticModelEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withNamespace(model.namespace())
            .withName(model.name())
            .withDefinition(
                SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build())
            .withProperties(Map.of())
            .withAuditInfo(AUDIT_INFO)
            .withRevision(1, status, null, null)
            .build();
    SemanticModelMetaService.getInstance().insertSemanticModel(entity, false);
    return entity;
  }

  private OntologyEntity createOntology() {
    return STORE.createOntology(
        RandomIdGenerator.INSTANCE,
        METALAKE,
        ONTOLOGY,
        null,
        definition("Customer"),
        Map.of(),
        ignored -> {});
  }

  private void createMapping() {
    createMapping(MAPPING);
  }

  private void createMapping(String name) {
    STORE.createMapping(
        RandomIdGenerator.INSTANCE,
        MODEL,
        name,
        null,
        ONTOLOGY,
        MAPPING_DEFINITION,
        Map.of(),
        ignored -> {});
  }

  private static OntologyMappingDefinition mappingDefinition(String expression) {
    return OntologyMappingDefinition.of(
        Map.of(
            "conceptMappings",
            List.of(
                Map.of(
                    "concept",
                    "Customer",
                    "objectMappings",
                    List.of(Map.of("expression", expression))))));
  }

  private static OntologyDefinition definition(String concept) {
    Map<String, Object> id =
        Map.of(
            "name",
            "id",
            "roles",
            List.of(Map.of("concept", "Integer")),
            "verbalizes",
            List.of("id"));
    Map<String, Object> entity =
        Map.of(
            "concept",
            concept,
            "type",
            "EntityType",
            "identifyBy",
            List.of("id"),
            "relationships",
            List.of(id));
    return OntologyDefinition.of(Map.of("ontology", List.of(entity)));
  }
}
