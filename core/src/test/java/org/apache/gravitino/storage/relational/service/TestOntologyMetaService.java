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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OssieOntologyDocuments;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Exercises the versioned ontology store on Gravitino's relational backends. */
public class TestOntologyMetaService extends TestJDBCBackend {
  private static final String METALAKE = "ontology_tests";
  private static final String ONTOLOGY = "Sales";
  private static final OntologyMetaService STORE = OntologyMetaService.getInstance();

  /** Creates the parent metalake. */
  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(METALAKE);
  }

  /** Checks ontology creation, replacement, stable IDs, renames, and dropping. */
  @TestTemplate
  public void testOntologyLifecycle() {
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
    assertEquals(List.of(ONTOLOGY), STORE.listOntologies(METALAKE));

    OntologyEntity renamed =
        STORE.alterOntology(METALAKE, ONTOLOGY, OntologyChange.rename("NewSales"));
    assertEquals(original.id(), renamed.id());
    assertEquals(0, STORE.retireVersions(1, 100));
    assertNotNull(renamed.auditInfo().lastModifiedTime());

    OntologyDefinition other = definition("Other");
    assertEquals(
        other,
        STORE
            .alterOntology(METALAKE, renamed.name(), OntologyChange.replaceDefinition(other))
            .definition());
    assertTrue(STORE.dropOntology(METALAKE, renamed.name()));
    assertFalse(STORE.dropOntology(METALAKE, renamed.name()));
  }

  /** Checks the metalake cascade. */
  @TestTemplate
  public void testMetalakeCascade() {
    OntologyEntity ontology = createOntology();
    assertTrue(MetalakeMetaService.getInstance().deleteMetalake(NameIdentifier.of(METALAKE), true));
    assertTrue(STORE.namesById(List.of(ontology.id())).isEmpty());
    assertEquals(2, STORE.purge(System.currentTimeMillis() + 1000, 100));
  }

  /** Verifies that repeating current values leaves the version history unchanged. */
  @TestTemplate
  public void testNoOpUpdatesDoNotCreateVersions() {
    createOntology();
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        OntologyChange.setProperty("owner", "team"),
        OntologyChange.updateComment("current"));
    OntologyEntity unchanged =
        STORE.alterOntology(
            METALAKE,
            ONTOLOGY,
            OntologyChange.setProperty("owner", "team"),
            OntologyChange.updateComment("current"),
            OntologyChange.removeProperty("missing"),
            OntologyChange.replaceDefinition(definition("Customer")));
    assertEquals("current", unchanged.comment());
    assertEquals(1, STORE.retireVersions(1, 100));
  }

  /** Covers rename conflicts and ID lookup. */
  @TestTemplate
  public void testRenameConflicts() {
    OntologyEntity sales = createOntology();
    STORE.createOntology(
        RandomIdGenerator.INSTANCE,
        METALAKE,
        "Other",
        null,
        definition("Customer"),
        Map.of(),
        ignored -> {});
    assertEquals(sales.id(), STORE.getId(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY)));
    assertThrows(
        NoSuchEntityException.class,
        () -> STORE.getId(NameIdentifierUtil.ofOntology(METALAKE, "Missing")));

    OntologyAlreadyExistsException ontologyConflict =
        assertThrows(
            OntologyAlreadyExistsException.class,
            () -> STORE.alterOntology(METALAKE, ONTOLOGY, OntologyChange.rename("Other")));
    assertTrue(ontologyConflict.getMessage().contains("'Other'"));
  }

  /** Verifies that removing the stored version makes the ontology export the pin. */
  @TestTemplate
  public void testRemoveOssieVersionFallsBackOnExport() {
    createOntology();
    String version = "0.3.0.dev9";
    STORE.alterOntology(METALAKE, ONTOLOGY, OntologyChange.setProperty(VERSION_PROPERTY, version));
    STORE.alterOntology(METALAKE, ONTOLOGY, OntologyChange.removeProperty(VERSION_PROPERTY));
    OntologyEntity ontology = STORE.getEntity(NameIdentifierUtil.ofOntology(METALAKE, ONTOLOGY));
    assertFalse(ontology.properties().containsKey(VERSION_PROPERTY));
    assertEquals(
        DEFAULT_VERSION,
        OssieOntologyDocuments.importOntology(
                OssieOntologyDocuments.exportOntology(ontology, OssieFormat.JSON))
            .properties()
            .get(VERSION_PROPERTY));
  }

  /** Verifies that reverting a replacement creates no version. */
  @TestTemplate
  public void testRevertingBatchCreatesNoVersion() {
    createOntology();
    STORE.alterOntology(
        METALAKE,
        ONTOLOGY,
        OntologyChange.setProperty("owner", "team"),
        OntologyChange.removeProperty("owner"),
        OntologyChange.replaceDefinition(definition("Other")),
        OntologyChange.replaceDefinition(definition("Customer")));
    assertEquals(0, STORE.retireVersions(1, 100));
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

  private static OntologyDefinition definition(String concept) {
    return OntologyDefinition.of(
        Map.of("ontology", List.of(Map.of("concept", concept, "type", "EntityType"))));
  }
}
