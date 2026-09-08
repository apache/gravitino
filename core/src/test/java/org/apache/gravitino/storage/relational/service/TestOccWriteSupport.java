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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.junit.jupiter.api.Test;

public class TestOccWriteSupport {

  private static class DummyPO {
    private final String name;
    private final Long parentId;

    DummyPO(String name, Long parentId) {
      this.name = name;
      this.parentId = parentId;
    }

    String name() {
      return name;
    }

    Long parentId() {
      return parentId;
    }
  }

  @Test
  void testOverwritePrefersNaturalKey() {
    DummyPO target = new DummyPO("target", 1L);
    assertSame(
        target,
        OccWriteSupport.findAndLockForOverwrite(
            () -> target,
            () -> {
              throw new AssertionError("ID lookup must be skipped");
            },
            po -> true));
  }

  @Test
  void testOverwriteFallsBackToIdInSameParent() {
    DummyPO target = new DummyPO("renamed", 1L);
    assertSame(
        target,
        OccWriteSupport.findAndLockForOverwrite(
            () -> null, () -> target, po -> po.parentId() == 1L));
  }

  @Test
  void testOverwriteRejectsIdInDifferentParent() {
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            OccWriteSupport.findAndLockForOverwrite(
                () -> null, () -> new DummyPO("foreign", 2L), po -> po.parentId() == 1L));
  }

  @Test
  void testOverwriteReturnsNullWhenBothLookupsMiss() {
    assertNull(
        OccWriteSupport.findAndLockForOverwrite(
            () -> null,
            () -> null,
            po -> {
              throw new AssertionError("No row to validate");
            }));
  }

  @Test
  void testWriteFailureReturnsNoSuchEntityWhenNotFound() {
    NameIdentifier ident = NameIdentifier.of("metalake_test");
    RuntimeException ex =
        OccWriteSupport.writeFailure(
            ident, Entity.EntityType.METALAKE, () -> null, null, po -> true);
    assertInstanceOf(NoSuchEntityException.class, ex);
  }

  @Test
  void testWriteFailureReturnsNoSuchEntityWhenIdentityMismatch() {
    NameIdentifier ident = NameIdentifier.of("metalake_test");
    DummyPO current = new DummyPO("different_name", 1L);
    RuntimeException ex =
        OccWriteSupport.writeFailure(
            ident,
            Entity.EntityType.METALAKE,
            () -> current,
            null,
            po -> Objects.equals(po.name(), "metalake_test"));
    assertInstanceOf(NoSuchEntityException.class, ex);
  }

  @Test
  void testWriteFailureUsesExplicitEntityName() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "model");
    RuntimeException ex =
        OccWriteSupport.writeFailure(
            ident, Entity.EntityType.MODEL, ident.toString(), () -> null, null, po -> true);

    assertEquals("No such model entity: metalake.catalog.schema.model", ex.getMessage());
  }

  @Test
  void testWriteFailureReturnsOptimisticLockExceptionWhenMatch() {
    NameIdentifier ident = NameIdentifier.of("metalake_test");
    DummyPO current = new DummyPO("metalake_test", 1L);
    RuntimeException ex =
        OccWriteSupport.writeFailure(
            ident,
            Entity.EntityType.METALAKE,
            () -> current,
            null,
            po -> Objects.equals(po.name(), "metalake_test"));
    assertInstanceOf(OptimisticLockException.class, ex);
  }

  @Test
  void testDeleteWithVersionSuccess() {
    assertDoesNotThrow(
        () ->
            OccWriteSupport.deleteWithVersion(
                () -> 1, () -> new RuntimeException("Should not be thrown")));
  }

  @Test
  void testDeleteWithVersionThrowsOnMiss() {
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OccWriteSupport.deleteWithVersion(
                () -> 0,
                () ->
                    new NoSuchEntityException(
                        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, "metalake", "test")));
  }

  @Test
  void testUpdateWithVersionSuccess() {
    assertDoesNotThrow(
        () ->
            OccWriteSupport.updateWithVersion(
                () -> 1, () -> new RuntimeException("Should not be thrown")));
  }

  @Test
  void testUpdateWithVersionThrowsOnMiss() {
    assertThrows(
        OptimisticLockException.class,
        () ->
            OccWriteSupport.updateWithVersion(
                () -> 0, () -> new OptimisticLockException("test conflict")));
  }

  @Test
  void testDeleteChildrenWithVersionsEmptyOrNull() {
    NameIdentifier parentIdent = NameIdentifier.of("parent");
    assertDoesNotThrow(
        () ->
            OccWriteSupport.deleteChildrenWithVersions(
                parentIdent,
                Entity.EntityType.CATALOG,
                Entity.EntityType.METALAKE,
                null,
                list -> 0));

    assertDoesNotThrow(
        () ->
            OccWriteSupport.deleteChildrenWithVersions(
                parentIdent,
                Entity.EntityType.CATALOG,
                Entity.EntityType.METALAKE,
                Collections.emptyList(),
                list -> 0));
  }

  @Test
  void testDeleteChildrenWithVersionsSuccess() {
    NameIdentifier parentIdent = NameIdentifier.of("parent");
    List<DummyPO> children = List.of(new DummyPO("c1", 1L), new DummyPO("c2", 1L));

    assertDoesNotThrow(
        () ->
            OccWriteSupport.deleteChildrenWithVersions(
                parentIdent,
                Entity.EntityType.CATALOG,
                Entity.EntityType.METALAKE,
                children,
                list -> 2));
  }

  @Test
  void testDeleteChildrenWithVersionsMismatchThrows() {
    NameIdentifier parentIdent = NameIdentifier.of("parent");
    List<DummyPO> children = List.of(new DummyPO("c1", 1L), new DummyPO("c2", 1L));

    assertThrows(
        OptimisticLockException.class,
        () ->
            OccWriteSupport.deleteChildrenWithVersions(
                parentIdent,
                Entity.EntityType.CATALOG,
                Entity.EntityType.METALAKE,
                children,
                list -> 1));
  }

  @Test
  void testLockParentForChildWriteSuccess() {
    DummyPO parent = new DummyPO("parent_name", 10L);
    DummyPO locked =
        OccWriteSupport.lockParentForChildWrite(
            "parent_name",
            Entity.EntityType.METALAKE,
            () -> parent,
            null,
            p -> Objects.equals(p.name(), "parent_name"));
    assertEquals(parent, locked);
  }

  @Test
  void testLockParentForChildWriteNotFoundThrows() {
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OccWriteSupport.lockParentForChildWrite(
                "parent_name", Entity.EntityType.METALAKE, () -> null, null, p -> true));
  }

  @Test
  void testLockParentForChildWriteIdentityMismatchThrows() {
    DummyPO parent = new DummyPO("wrong_parent_name", 10L);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            OccWriteSupport.lockParentForChildWrite(
                "parent_name",
                Entity.EntityType.METALAKE,
                () -> parent,
                null,
                p -> Objects.equals(p.name(), "parent_name")));
  }
}
