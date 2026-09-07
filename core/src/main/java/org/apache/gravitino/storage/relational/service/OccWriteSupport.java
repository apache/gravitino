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

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;

/**
 * Utility class providing shared helpers for optimistic concurrency control (OCC) operations across
 * relational metadata services.
 */
public class OccWriteSupport {

  private OccWriteSupport() {}

  /**
   * Classifies a write-failure for an entity during an optimistic concurrency control operation.
   *
   * <p>Executes a lookup to re-read the target entity. The caller is responsible for choosing the
   * lookup semantics needed by its transaction, usually a locking read by stable ID. If the entity
   * no longer exists or its natural key fields do not match the expected identity, returns a {@link
   * NoSuchEntityException}. Otherwise, returns an {@link
   * org.apache.gravitino.exceptions.OptimisticLockException} via {@link
   * ExceptionUtils#concurrentModification(Entity.EntityType, NameIdentifier)}.
   *
   * @param <T> the persistent object (PO) type of the entity
   * @param identifier the name identifier of the entity
   * @param type the entity type
   * @param currentLookup a supplier that retrieves the current entity
   * @param poMapper an optional function to transform the retrieved PO (e.g. physical to logical)
   * @param sameIdentity a predicate comparing the retrieved PO against expected natural key values
   * @return the classified RuntimeException to be thrown
   */
  public static <T> RuntimeException writeFailure(
      NameIdentifier identifier,
      Entity.EntityType type,
      Supplier<T> currentLookup,
      @Nullable Function<T, T> poMapper,
      @Nullable Predicate<T> sameIdentity) {
    return writeFailure(identifier, type, identifier.name(), currentLookup, poMapper, sameIdentity);
  }

  /**
   * Classifies a write-failure while preserving a caller-specific entity name in not-found errors.
   *
   * <p>This overload is useful for services whose existing error contract reports a fully qualified
   * name rather than {@link NameIdentifier#name()}.
   *
   * @param <T> the persistent object (PO) type of the entity
   * @param identifier the name identifier used for an optimistic-lock error
   * @param type the entity type
   * @param entityName the entity name used for a not-found error
   * @param currentLookup a supplier that retrieves the current entity
   * @param poMapper an optional function to transform the retrieved PO (e.g. physical to logical)
   * @param sameIdentity a predicate comparing the retrieved PO against expected natural key values
   * @return the classified RuntimeException to be thrown
   */
  public static <T> RuntimeException writeFailure(
      NameIdentifier identifier,
      Entity.EntityType type,
      String entityName,
      Supplier<T> currentLookup,
      @Nullable Function<T, T> poMapper,
      @Nullable Predicate<T> sameIdentity) {
    T currentPO = currentLookup.get();
    if (currentPO != null && poMapper != null) {
      currentPO = poMapper.apply(currentPO);
    }
    if (currentPO == null || (sameIdentity != null && !sameIdentity.test(currentPO))) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          type.name().toLowerCase(Locale.ROOT),
          entityName);
    }
    return ExceptionUtils.concurrentModification(type, identifier);
  }

  /**
   * Executes a single-row compare-and-set soft delete for an entity guarded by version.
   *
   * @param softDeleteOps an operation supplying the number of rows affected by soft delete
   * @param onMissSupplier a supplier providing the RuntimeException when zero rows are deleted
   */
  public static void deleteWithVersion(
      IntSupplier softDeleteOps, Supplier<RuntimeException> onMissSupplier) {
    int deleted = softDeleteOps.getAsInt();
    if (deleted == 0) {
      throw onMissSupplier.get();
    }
  }

  /**
   * Executes a batch soft delete of child entities guarded by their individual versions.
   *
   * <p>If the affected row count does not match the size of the child list, throws a concurrent
   * child modification exception via {@link ExceptionUtils#concurrentChildModification}.
   *
   * @param <T> the persistent object (PO) type of the child entities
   * @param parentIdentifier the name identifier of the parent entity
   * @param childType the entity type of the children
   * @param parentType the entity type of the parent
   * @param children the list of child persistent objects to delete
   * @param softDeleteOps a function performing batch soft delete on the child list
   */
  public static <T> void deleteChildrenWithVersions(
      NameIdentifier parentIdentifier,
      Entity.EntityType childType,
      Entity.EntityType parentType,
      @Nullable List<T> children,
      ToIntFunction<List<T>> softDeleteOps) {
    if (children == null || children.isEmpty()) {
      return;
    }
    int deleted = softDeleteOps.applyAsInt(children);
    if (deleted != children.size()) {
      throw ExceptionUtils.concurrentChildModification(childType, parentType, parentIdentifier);
    }
  }

  /**
   * Locks the parent row before executing a child write or delete operation to guarantee existence
   * and identity consistency.
   *
   * @param <P> the persistent object (PO) type of the parent entity
   * @param parentEntityName the name of the parent entity expected in exceptions
   * @param parentType the entity type of the parent
   * @param lockingLookup a supplier that retrieves the parent entity while locking its row
   * @param poMapper an optional function to transform the retrieved parent PO
   * @param sameParentIdentity a predicate verifying parent identity (e.g. name and ancestor ids)
   * @return the locked parent persistent object
   * @throws NoSuchEntityException if the parent entity is missing or fails identity validation
   */
  public static <P> P lockParentForChildWrite(
      String parentEntityName,
      Entity.EntityType parentType,
      Supplier<P> lockingLookup,
      @Nullable Function<P, P> poMapper,
      @Nullable Predicate<P> sameParentIdentity) {
    P currentParent = lockingLookup.get();
    if (currentParent != null && poMapper != null) {
      currentParent = poMapper.apply(currentParent);
    }
    if (currentParent == null
        || (sameParentIdentity != null && !sameParentIdentity.test(currentParent))) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          parentType.name().toLowerCase(Locale.ROOT),
          parentEntityName);
    }
    return currentParent;
  }
}
