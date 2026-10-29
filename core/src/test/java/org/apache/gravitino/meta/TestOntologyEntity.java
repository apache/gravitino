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
package org.apache.gravitino.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.ontology.ConceptType;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyConcept;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.semantic.AIContext;
import org.junit.jupiter.api.Test;

/** Tests the {@link OntologyEntity} contract. */
public class TestOntologyEntity {

  /** The entity exposes every declared field and the metalake-level identifier. */
  @Test
  public void testEntityFields() {
    OntologyDefinition definition = definition();
    AuditInfo auditInfo = auditInfo();
    Map<String, String> properties = Map.of("owner", "analytics");

    OntologyEntity entity =
        OntologyEntity.builder()
            .withId(1L)
            .withName("finance")
            .withNamespace(Namespace.of("metalake"))
            .withComment("Finance vocabulary")
            .withStatus(Ontology.Status.DRAFT)
            .withDefinition(definition)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    assertEquals(1L, entity.id());
    assertEquals("finance", entity.name());
    assertEquals(Namespace.of("metalake"), entity.namespace());
    assertEquals("Finance vocabulary", entity.comment());
    assertEquals(Ontology.Status.DRAFT, entity.status());
    assertSame(definition, entity.definition());
    assertEquals(properties, entity.properties());
    assertEquals(auditInfo, entity.auditInfo());
    assertEquals(Entity.EntityType.ONTOLOGY, entity.type());
    assertEquals(NameIdentifier.of("metalake", "finance"), entity.nameIdentifier());
    assertEquals(7, entity.fields().size());
    assertSame(definition, entity.fields().get(OntologyEntity.DEFINITION));
    assertEquals(Ontology.Status.DRAFT, entity.fields().get(OntologyEntity.STATUS));
  }

  /** Optional fields default instead of failing validation. */
  @Test
  public void testOptionalFieldsDefault() {
    OntologyEntity entity =
        OntologyEntity.builder()
            .withId(2L)
            .withName("minimal")
            .withNamespace(Namespace.of("metalake"))
            .withStatus(Ontology.Status.PUBLISHED)
            .withDefinition(definition())
            .withComment(null)
            .withProperties(null)
            .withAuditInfo(auditInfo())
            .build();

    assertNull(entity.comment());
    assertEquals(Map.of(), entity.properties());
    assertEquals(Ontology.Status.PUBLISHED, entity.status());
    assertNull(entity.fields().get(OntologyEntity.COMMENT));
  }

  /** A missing required field is rejected by validation. */
  @Test
  public void testValidationRejectsMissingRequiredFields() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyEntity.builder()
                .withName("no-id")
                .withNamespace(Namespace.of("metalake"))
                .withStatus(Ontology.Status.DRAFT)
                .withDefinition(definition())
                .withAuditInfo(auditInfo())
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyEntity.builder()
                .withId(3L)
                .withNamespace(Namespace.of("metalake"))
                .withStatus(Ontology.Status.DRAFT)
                .withDefinition(definition())
                .withAuditInfo(auditInfo())
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyEntity.builder()
                .withId(3L)
                .withName("no-status")
                .withNamespace(Namespace.of("metalake"))
                .withDefinition(definition())
                .withAuditInfo(auditInfo())
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyEntity.builder()
                .withId(3L)
                .withName("no-definition")
                .withNamespace(Namespace.of("metalake"))
                .withStatus(Ontology.Status.DRAFT)
                .withAuditInfo(auditInfo())
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            OntologyEntity.builder()
                .withId(3L)
                .withName("no-audit")
                .withNamespace(Namespace.of("metalake"))
                .withStatus(Ontology.Status.DRAFT)
                .withDefinition(definition())
                .build());
  }

  /** The namespace is required even though it is not one of the declared fields. */
  @Test
  public void testValidationRejectsMissingNamespace() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                OntologyEntity.builder()
                    .withId(4L)
                    .withName("no-namespace")
                    .withStatus(Ontology.Status.DRAFT)
                    .withDefinition(definition())
                    .withAuditInfo(auditInfo())
                    .build());
    assertTrue(exception.getMessage().contains("namespace"));
  }

  /** The entity reports a mismatch between its id and a field value. */
  @Test
  public void testValidationRejectsWrongFieldType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OntologyEntity.builder().withId(null).withName("x").build());
  }

  /** Equality and hash codes compare every field by value. */
  @Test
  public void testEqualsAndHashCode() {
    OntologyDefinition definition = definition();
    AuditInfo auditInfo = auditInfo();

    OntologyEntity first = entity(1L, "finance", Ontology.Status.DRAFT, definition, auditInfo);
    OntologyEntity equal = entity(1L, "finance", Ontology.Status.DRAFT, definition, auditInfo);
    OntologyEntity otherId = entity(2L, "finance", Ontology.Status.DRAFT, definition, auditInfo);
    OntologyEntity otherStatus =
        entity(1L, "finance", Ontology.Status.PUBLISHED, definition, auditInfo);

    assertEquals(first, equal);
    assertEquals(first.hashCode(), equal.hashCode());

    assertNotEquals(first, otherId);
    assertNotEquals(first, otherStatus);
    assertNotEquals(first, null);
    assertNotEquals(first, "finance");
  }

  /** The entity string form names the type and its identifying fields. */
  @Test
  public void testToString() {
    String text =
        entity(1L, "finance", Ontology.Status.DRAFT, definition(), auditInfo()).toString();

    assertTrue(text.contains("OntologyEntity"));
    assertTrue(text.contains("finance"));
    assertTrue(text.contains("DRAFT"));
  }

  /** The properties map cannot mutate the entity snapshot. */
  @Test
  public void testPropertiesAreDefensivelyCopied() {
    Map<String, String> properties = new HashMap<>();
    properties.put("owner", "analytics");

    OntologyEntity entity =
        OntologyEntity.builder()
            .withId(5L)
            .withName("copied")
            .withNamespace(Namespace.of("metalake"))
            .withStatus(Ontology.Status.DRAFT)
            .withDefinition(definition())
            .withProperties(properties)
            .withAuditInfo(auditInfo())
            .build();

    properties.put("owner", "changed");
    assertEquals("analytics", entity.properties().get("owner"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> entity.properties().put("new-property", "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> entity.fields().put(OntologyEntity.COMMENT, "changed"));
  }

  private static OntologyEntity entity(
      Long id,
      String name,
      Ontology.Status status,
      OntologyDefinition definition,
      AuditInfo auditInfo) {
    return OntologyEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(Namespace.of("metalake"))
        .withComment("Finance vocabulary")
        .withStatus(status)
        .withDefinition(definition)
        .withProperties(Map.of("owner", "analytics"))
        .withAuditInfo(auditInfo)
        .build();
  }

  private static OntologyDefinition definition() {
    return OntologyDefinition.builder()
        .withAiContext(AIContext.of("Certified"))
        .withRequires("Account IS NOT NULL")
        .withConcepts(
            OntologyConcept.builder()
                .withConcept("Account")
                .withType(ConceptType.ENTITY_TYPE)
                .build(),
            OntologyConcept.builder()
                .withConcept("Balance")
                .withType(ConceptType.VALUE_TYPE)
                .build())
        .withPrefixes(Map.of("ex", "http://example.com/finance/"))
        .build();
  }

  private static AuditInfo auditInfo() {
    return AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  }
}
