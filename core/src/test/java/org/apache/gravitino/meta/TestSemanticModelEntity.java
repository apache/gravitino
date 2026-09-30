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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.junit.jupiter.api.Test;

/** Tests for {@link SemanticModelEntity}. */
public class TestSemanticModelEntity {

  /** Tests the complete entity field contract and optional defaults. */
  @Test
  public void testSemanticModelEntityFields() {
    SemanticModelDefinition definition = completeDefinition();
    AuditInfo auditInfo = auditInfo();
    Map<String, String> properties = Map.of("owner", "analytics");

    SemanticModelEntity entity =
        SemanticModelEntity.builder()
            .withId(1L)
            .withName("sales")
            .withNamespace(Namespace.of("metalake", "catalog", "schema"))
            .withComment("Sales model")
            .withDefinition(definition)
            .withProperties(properties)
            .withAuditInfo(auditInfo)
            .build();

    assertEquals(1L, entity.id());
    assertEquals("sales", entity.name());
    assertEquals(Namespace.of("metalake", "catalog", "schema"), entity.namespace());
    assertEquals("Sales model", entity.comment());
    assertSame(definition, entity.definition());
    assertEquals(properties, entity.properties());
    assertEquals(auditInfo, entity.auditInfo());
    assertEquals(Entity.EntityType.SEMANTIC_MODEL, entity.type());
    assertSame(definition, entity.fields().get(SemanticModelEntity.DEFINITION));

    SemanticModelDefinition minimalDefinition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset("orders")}).build();
    SemanticModelEntity minimalEntity =
        SemanticModelEntity.builder()
            .withId(2L)
            .withName("minimal")
            .withNamespace(Namespace.of("metalake", "catalog", "schema"))
            .withDefinition(minimalDefinition)
            .withProperties(null)
            .withAuditInfo(auditInfo)
            .build();

    assertNull(minimalEntity.comment());
    assertNull(minimalEntity.definition().aiContext());
    assertNull(minimalEntity.definition().relationships());
    assertNull(minimalEntity.definition().metrics());
    assertNull(minimalEntity.definition().customExtensions());
    assertEquals(Map.of(), minimalEntity.properties());
  }

  /** Tests that absent and explicitly empty optional arrays remain distinguishable. */
  @Test
  public void testPreservesNullAndEmptyOptionalArrays() {
    Dataset dataset = dataset("orders");
    SemanticModelDefinition absent =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
    SemanticModelDefinition empty =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset})
            .withRelationships(new Relationship[0])
            .withMetrics(new Metric[0])
            .withCustomExtensions(new CustomExtension[0])
            .build();

    SemanticModelEntity absentEntity = entity("absent", absent, Map.of());
    SemanticModelEntity emptyEntity = entity("empty", empty, Map.of());

    assertNull(absentEntity.definition().relationships());
    assertNull(absentEntity.definition().metrics());
    assertNull(absentEntity.definition().customExtensions());
    assertArrayEquals(new Relationship[0], emptyEntity.definition().relationships());
    assertArrayEquals(new Metric[0], emptyEntity.definition().metrics());
    assertArrayEquals(new CustomExtension[0], emptyEntity.definition().customExtensions());
    assertNotEquals(absentEntity.definition(), emptyEntity.definition());
  }

  /** Tests that definition arrays and properties cannot mutate the entity snapshot. */
  @Test
  public void testDefensivelyCopiesStructuredCollections() {
    Dataset[] datasets = {dataset("orders")};
    Relationship[] relationships = {relationship("orders_to_customers")};
    Metric[] metrics = {metric("order_count")};
    CustomExtension[] extensions = {extension("example")};
    Map<String, String> properties = new HashMap<>();
    properties.put("owner", "analytics");

    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of("Certified sales definitions"))
            .withDatasets(datasets)
            .withRelationships(relationships)
            .withMetrics(metrics)
            .withCustomExtensions(extensions)
            .build();
    SemanticModelEntity entity = entity("sales", definition, properties);

    datasets[0] = dataset("changed");
    relationships[0] = relationship("changed");
    metrics[0] = metric("changed");
    extensions[0] = extension("changed");
    properties.put("owner", "changed");

    assertEquals("orders", entity.definition().datasets()[0].name());
    assertEquals("orders_to_customers", entity.definition().relationships()[0].name());
    assertEquals("order_count", entity.definition().metrics()[0].name());
    assertEquals("example", entity.definition().customExtensions()[0].vendorName());
    assertEquals("analytics", entity.properties().get("owner"));

    Dataset[] returnedDatasets = entity.definition().datasets();
    Relationship[] returnedRelationships = entity.definition().relationships();
    Metric[] returnedMetrics = entity.definition().metrics();
    CustomExtension[] returnedExtensions = entity.definition().customExtensions();
    returnedDatasets[0] = dataset("returned");
    returnedRelationships[0] = relationship("returned");
    returnedMetrics[0] = metric("returned");
    returnedExtensions[0] = extension("returned");

    assertEquals("orders", entity.definition().datasets()[0].name());
    assertEquals("orders_to_customers", entity.definition().relationships()[0].name());
    assertEquals("order_count", entity.definition().metrics()[0].name());
    assertEquals("example", entity.definition().customExtensions()[0].vendorName());
    assertNotSame(entity.definition().datasets(), entity.definition().datasets());
    assertThrows(
        UnsupportedOperationException.class,
        () -> entity.properties().put("new-property", "value"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> entity.fields().put(SemanticModelEntity.COMMENT, "changed"));
  }

  /** Tests value-based equality and hash codes. */
  @Test
  public void testEqualsAndHashCode() {
    SemanticModelDefinition definition = completeDefinition();
    AuditInfo auditInfo = auditInfo();

    SemanticModelEntity first =
        SemanticModelEntity.builder()
            .withId(1L)
            .withName("sales")
            .withNamespace(Namespace.of("metalake", "catalog", "schema"))
            .withComment("Sales model")
            .withDefinition(definition)
            .withProperties(Map.of("owner", "analytics"))
            .withAuditInfo(auditInfo)
            .build();
    SemanticModelEntity equal =
        SemanticModelEntity.builder()
            .withId(1L)
            .withName("sales")
            .withNamespace(Namespace.of("metalake", "catalog", "schema"))
            .withComment("Sales model")
            .withDefinition(definition)
            .withProperties(Map.of("owner", "analytics"))
            .withAuditInfo(auditInfo)
            .build();
    SemanticModelEntity different = entity("marketing", definition, Map.of());

    assertEquals(first, equal);
    assertEquals(first.hashCode(), equal.hashCode());
    assertNotEquals(first, different);
    assertNotEquals(first, new Object());
  }

  /** Tests validation of required entity fields. */
  @Test
  public void testRequiredFieldValidation() {
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset("orders")}).build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModelEntity.builder()
                .withId(1L)
                .withName("sales")
                .withDefinition(definition)
                .withAuditInfo(auditInfo())
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            SemanticModelEntity.builder()
                .withId(1L)
                .withName("sales")
                .withNamespace(Namespace.of("metalake", "catalog", "schema"))
                .withAuditInfo(auditInfo())
                .build());
  }

  private static SemanticModelEntity entity(
      String name, SemanticModelDefinition definition, Map<String, String> properties) {
    return SemanticModelEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(Namespace.of("metalake", "catalog", "schema"))
        .withComment("Sales model")
        .withDefinition(definition)
        .withProperties(properties)
        .withAuditInfo(auditInfo())
        .build();
  }

  private static SemanticModelDefinition completeDefinition() {
    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of("Certified sales definitions"))
        .withDatasets(new Dataset[] {dataset("orders")})
        .withRelationships(new Relationship[] {relationship("orders_to_customers")})
        .withMetrics(new Metric[] {metric("order_count")})
        .withCustomExtensions(new CustomExtension[] {extension("example")})
        .build();
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }

  private static Relationship relationship(String name) {
    return Relationship.builder()
        .withName(name)
        .withFrom("orders")
        .withTo("customers")
        .withFromColumns(new String[] {"customer_id"})
        .withToColumns(new String[] {"id"})
        .build();
  }

  private static Metric metric(String name) {
    return Metric.builder()
        .withName(name)
        .withExpression(
            Expression.builder()
                .withDialects(
                    new DialectExpression[] {
                      DialectExpression.builder()
                          .withDialect(Dialects.ANSI_SQL)
                          .withExpression("COUNT(*)")
                          .build()
                    })
                .build())
        .build();
  }

  private static CustomExtension extension(String vendorName) {
    return CustomExtension.builder().withVendorName(vendorName).withData("{}").build();
  }

  private static AuditInfo auditInfo() {
    return AuditInfo.builder()
        .withCreator("tester")
        .withCreateTime(Instant.parse("2026-08-11T00:00:00Z"))
        .build();
  }
}
