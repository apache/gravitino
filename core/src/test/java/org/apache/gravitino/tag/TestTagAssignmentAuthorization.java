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
package org.apache.gravitino.tag;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.junit.jupiter.api.Test;

class TestTagAssignmentAuthorization {
  @Test
  void testScopeAndNestedScope() {
    NameIdentifier tag = NameIdentifier.of("metalake", "tag");
    NameIdentifier other = NameIdentifier.of("metalake", "other");
    TagAssignmentAuthorization.requireDescriptive(tag);
    TagAssignmentAuthorization.recordIdentity(tag, 1L);
    assertFalse(TagAssignmentAuthorization.isActive());
    assertNull(TagAssignmentAuthorization.checkedIdentity(tag));
    assertFalse(TagAssignmentAuthorization.requiresDescriptive(tag));
    try (TagAssignmentAuthorization outer = TagAssignmentAuthorization.open()) {
      assertTrue(TagAssignmentAuthorization.isActive());
      TagAssignmentAuthorization.recordIdentity(tag, 1L);
      TagAssignmentAuthorization.recordIdentity(tag, 2L);
      assertEquals(1L, TagAssignmentAuthorization.checkedIdentity(tag));
      TagAssignmentAuthorization.requireDescriptive(tag);
      assertTrue(TagAssignmentAuthorization.requiresDescriptive(tag));
      assertFalse(TagAssignmentAuthorization.requiresDescriptive(other));
      try (TagAssignmentAuthorization inner = TagAssignmentAuthorization.open()) {
        assertNull(TagAssignmentAuthorization.checkedIdentity(tag));
        assertFalse(TagAssignmentAuthorization.requiresDescriptive(tag));
        TagAssignmentAuthorization.requireDescriptive(other);
        assertTrue(TagAssignmentAuthorization.requiresDescriptive(other));
      }
      assertTrue(TagAssignmentAuthorization.requiresDescriptive(tag));
      assertEquals(1L, TagAssignmentAuthorization.checkedIdentity(tag));
      assertFalse(TagAssignmentAuthorization.requiresDescriptive(other));
    }
    assertFalse(TagAssignmentAuthorization.requiresDescriptive(tag));
    assertFalse(TagAssignmentAuthorization.isActive());
    assertNull(TagAssignmentAuthorization.checkedIdentity(tag));
  }

  @Test
  void testTargetAndAncestorIdentity() {
    NameIdentifier catalog = NameIdentifier.of("metalake", "catalog");
    NamespacedEntityId original = new NamespacedEntityId(2L, 1L);
    NamespacedEntityId replacement = new NamespacedEntityId(3L, 1L);
    NamespacedEntityId changedAncestor = new NamespacedEntityId(2L, 4L);
    assertDoesNotThrow(
        () ->
            TagAssignmentAuthorization.checkTarget(
                catalog, Entity.EntityType.CATALOG, replacement));
    try (TagAssignmentAuthorization scope = TagAssignmentAuthorization.open()) {
      TagAssignmentAuthorization.recordTarget(catalog, Entity.EntityType.CATALOG, original);
      assertTrue(TagAssignmentAuthorization.hasTargetType(MetadataObject.Type.CATALOG));
      assertTrue(TagAssignmentAuthorization.hasTargetType(MetadataObject.Type.TABLE));
      assertEquals(
          2L,
          TagAssignmentAuthorization.checkedTargetIdentity(catalog, MetadataObject.Type.CATALOG));
      assertNull(
          TagAssignmentAuthorization.checkedTargetIdentity(
              NameIdentifier.of("metalake", "other"), MetadataObject.Type.CATALOG));
      assertDoesNotThrow(
          () ->
              TagAssignmentAuthorization.checkTarget(catalog, Entity.EntityType.CATALOG, original));
      assertThrows(
          OptimisticLockException.class,
          () ->
              TagAssignmentAuthorization.checkTarget(
                  catalog, Entity.EntityType.CATALOG, replacement));
      assertThrows(
          OptimisticLockException.class,
          () ->
              TagAssignmentAuthorization.checkTarget(
                  catalog, Entity.EntityType.CATALOG, changedAncestor));
      assertThrows(
          OptimisticLockException.class,
          () -> TagAssignmentAuthorization.checkTarget(catalog, Entity.EntityType.TABLE, original));
      TagAssignmentAuthorization.recordTarget(catalog, Entity.EntityType.CATALOG, null);
      assertNull(
          TagAssignmentAuthorization.checkedTargetIdentity(catalog, MetadataObject.Type.CATALOG));
      assertThrows(
          OptimisticLockException.class,
          () ->
              TagAssignmentAuthorization.checkTarget(catalog, Entity.EntityType.CATALOG, original));
      assertTrue(TagAssignmentAuthorization.wasCheckedAbsent(catalog, MetadataObject.Type.CATALOG));
      TagAssignmentAuthorization.recordMissingTargetAncestor(new NamespacedEntityId(1L));
      assertEquals(
          1L,
          TagAssignmentAuthorization.checkedTargetIdentity(
              NameIdentifier.of("metalake"), MetadataObject.Type.METALAKE));
      assertFalse(
          TagAssignmentAuthorization.wasCheckedAbsent(
              NameIdentifier.of("metalake"), MetadataObject.Type.METALAKE));
      assertDoesNotThrow(
          () ->
              TagAssignmentAuthorization.checkTarget(catalog, Entity.EntityType.CATALOG, original));
      assertThrows(
          OptimisticLockException.class,
          () ->
              TagAssignmentAuthorization.checkTarget(
                  catalog, Entity.EntityType.CATALOG, changedAncestor));
      NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "table");
      TagAssignmentAuthorization.recordTarget(
          table, Entity.EntityType.TABLE, new NamespacedEntityId(4L, 1L, 2L, 3L));
      assertEquals(
          1L,
          TagAssignmentAuthorization.checkedTargetIdentity(
              NameIdentifier.of("metalake"), MetadataObject.Type.METALAKE));
      assertEquals(
          2L,
          TagAssignmentAuthorization.checkedTargetIdentity(catalog, MetadataObject.Type.CATALOG));
      assertEquals(
          3L,
          TagAssignmentAuthorization.checkedTargetIdentity(
              NameIdentifier.of("metalake", "catalog", "schema"), MetadataObject.Type.SCHEMA));
      assertNull(
          TagAssignmentAuthorization.checkedTargetIdentity(
              NameIdentifier.of("metalake", "other"), MetadataObject.Type.CATALOG));
    }
    assertDoesNotThrow(
        () ->
            TagAssignmentAuthorization.checkTarget(
                catalog, Entity.EntityType.CATALOG, replacement));
  }

  @Test
  void testSchemaAncestorSnapshots() {
    NameIdentifier target = NameIdentifier.of("metalake", "catalog", "outer:inner", "table");
    NameIdentifier outer = NameIdentifier.of("metalake", "catalog", "outer");
    NameIdentifier inner = NameIdentifier.of("metalake", "catalog", "outer:inner");
    TagAssignmentAuthorization.recordSchemaAncestor(outer, new NamespacedEntityId(3L, 1L, 2L));
    assertTrue(TagAssignmentAuthorization.checkedSchemaAncestors().isEmpty());
    try (TagAssignmentAuthorization scope = TagAssignmentAuthorization.open()) {
      TagAssignmentAuthorization.recordTarget(target, Entity.EntityType.TABLE, null);
      TagAssignmentAuthorization.recordSchemaAncestor(outer, new NamespacedEntityId(3L, 1L, 2L));
      TagAssignmentAuthorization.recordSchemaAncestor(inner, null);
      assertEquals(
          List.of(outer, inner),
          new ArrayList<>(TagAssignmentAuthorization.checkedSchemaAncestors().keySet()));
      assertEquals(
          3L, TagAssignmentAuthorization.checkedTargetIdentity(outer, MetadataObject.Type.SCHEMA));
      assertFalse(TagAssignmentAuthorization.wasCheckedAbsent(outer, MetadataObject.Type.SCHEMA));
      assertTrue(TagAssignmentAuthorization.wasCheckedAbsent(inner, MetadataObject.Type.SCHEMA));
      try (TagAssignmentAuthorization nested = TagAssignmentAuthorization.open()) {
        assertTrue(TagAssignmentAuthorization.checkedSchemaAncestors().isEmpty());
      }
      assertEquals(
          List.of(outer, inner),
          new ArrayList<>(TagAssignmentAuthorization.checkedSchemaAncestors().keySet()));
    }
    assertTrue(TagAssignmentAuthorization.checkedSchemaAncestors().isEmpty());
  }
}
