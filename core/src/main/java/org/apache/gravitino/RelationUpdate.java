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
package org.apache.gravitino;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Describes an immutable relation update from a source entity to target endpoints. Target endpoint
 * identity and relation-edge attributes are carried by immutable {@link RelationEdgeTarget}
 * instances.
 *
 * <p>The target endpoint arrays are copied when the update is created and when the targets are
 * returned. The copy protects the array container from external mutation; the elements themselves
 * are immutable.
 */
public final class RelationUpdate {

  private static final RelationEdgeTarget[] EMPTY_TARGETS = new RelationEdgeTarget[0];

  private final SupportsRelationOperations.Type relationType;
  private final NameIdentifier sourceIdentifier;
  private final Entity.EntityType sourceEntityType;
  private final RelationEdgeTarget[] targetsToAdd;
  private final RelationEdgeTarget[] targetsToRemove;
  @Nullable private final Audit auditInfo;
  @Nullable private final Long expectedSourceId;

  private RelationUpdate(
      SupportsRelationOperations.Type relationType,
      NameIdentifier sourceIdentifier,
      Entity.EntityType sourceEntityType,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove,
      @Nullable Audit auditInfo,
      @Nullable Long expectedSourceId) {
    this.relationType = relationType;
    this.sourceIdentifier = sourceIdentifier;
    this.sourceEntityType = sourceEntityType;
    this.targetsToAdd = copyTargets(targetsToAdd, "targetsToAdd");
    this.targetsToRemove = copyTargets(targetsToRemove, "targetsToRemove");
    this.auditInfo = auditInfo;
    this.expectedSourceId = expectedSourceId;
  }

  /**
   * Creates a relation update.
   *
   * @param relationType The type of relation.
   * @param sourceIdentifier The identifier of the source entity whose relations are being updated.
   * @param sourceEntityType The source entity type.
   * @param targetsToAdd Target endpoints to associate with the source entity.
   * @param targetsToRemove Target endpoints to disassociate from the source entity.
   * @return A relation update.
   */
  public static RelationUpdate of(
      SupportsRelationOperations.Type relationType,
      NameIdentifier sourceIdentifier,
      Entity.EntityType sourceEntityType,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove) {
    Preconditions.checkArgument(relationType != null, "relationType must not be null");
    Preconditions.checkArgument(sourceIdentifier != null, "sourceIdentifier must not be null");
    Preconditions.checkArgument(sourceEntityType != null, "sourceEntityType must not be null");
    return new RelationUpdate(
        relationType,
        sourceIdentifier,
        sourceEntityType,
        targetsToAdd,
        targetsToRemove,
        null,
        null);
  }

  /**
   * Returns an update with explicit audit metadata for newly created relations.
   *
   * @param auditInfo The audit metadata to preserve, for example during an offline migration.
   * @return A copy carrying the supplied audit metadata.
   * @throws IllegalArgumentException If auditInfo is null.
   */
  public RelationUpdate withAuditInfo(Audit auditInfo) {
    Preconditions.checkArgument(auditInfo != null, "auditInfo must not be null");
    return new RelationUpdate(
        relationType,
        sourceIdentifier,
        sourceEntityType,
        targetsToAdd,
        targetsToRemove,
        auditInfo,
        expectedSourceId);
  }

  /**
   * Conditions initial owner assignment on the identity returned by creation.
   *
   * <p>Currently supported only for {@code OWNER_REL} updates whose source is a table, with one
   * USER target and no removals, relation values or explicit audit metadata. The relational store
   * locks the active source row by this ID and inserts only if it has no live owner relation.
   * Missing/deleted sources and existing owners return an empty result; a successful insert returns
   * the assigned user. Principal and storage failures propagate. Other relation updates must not
   * silently ignore the condition.
   *
   * @param expectedSourceId the original entity ID, never re-resolved by source name
   * @return a copy carrying the expected source ID
   * @throws IllegalArgumentException if this is not an OWNER_REL update
   */
  public RelationUpdate withExpectedSourceId(long expectedSourceId) {
    Preconditions.checkArgument(
        relationType == SupportsRelationOperations.Type.OWNER_REL,
        "Expected source identity is currently supported only for OWNER_REL");
    return new RelationUpdate(
        relationType,
        sourceIdentifier,
        sourceEntityType,
        targetsToAdd,
        targetsToRemove,
        auditInfo,
        expectedSourceId);
  }

  /**
   * @return the optional source identity condition for initial owner assignment
   */
  public Optional<Long> expectedSourceId() {
    return Optional.ofNullable(expectedSourceId);
  }

  /**
   * @return Explicit creation audit metadata, or empty to use the current principal and time.
   */
  public Optional<Audit> auditInfo() {
    return Optional.ofNullable(auditInfo);
  }

  /**
   * @return The type of relation.
   */
  public SupportsRelationOperations.Type relationType() {
    return relationType;
  }

  /**
   * @return The source entity identifier.
   */
  public NameIdentifier sourceIdentifier() {
    return sourceIdentifier;
  }

  /**
   * @return The source entity type.
   */
  public Entity.EntityType sourceEntityType() {
    return sourceEntityType;
  }

  /**
   * @return A copy of the target endpoint array to associate with the source entity.
   */
  public RelationEdgeTarget[] targetsToAdd() {
    return targetsToAdd.clone();
  }

  /**
   * @return A copy of the target endpoint array to disassociate from the source entity.
   */
  public RelationEdgeTarget[] targetsToRemove() {
    return targetsToRemove.clone();
  }

  /**
   * @return Whether any target endpoint carries relation values.
   */
  public boolean hasRelationValues() {
    return Arrays.stream(targetsToAdd).anyMatch(target -> target.relationValue().isPresent())
        || Arrays.stream(targetsToRemove).anyMatch(target -> target.relationValue().isPresent());
  }

  private static RelationEdgeTarget[] copyTargets(
      RelationEdgeTarget[] targets, String parameterName) {
    if (targets == null) {
      return EMPTY_TARGETS;
    }

    for (RelationEdgeTarget target : targets) {
      Preconditions.checkArgument(target != null, "%s must not contain null", parameterName);
    }

    return targets.clone();
  }
}
