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

  private RelationUpdate(
      SupportsRelationOperations.Type relationType,
      NameIdentifier sourceIdentifier,
      Entity.EntityType sourceEntityType,
      RelationEdgeTarget[] targetsToAdd,
      RelationEdgeTarget[] targetsToRemove) {
    this.relationType = relationType;
    this.sourceIdentifier = sourceIdentifier;
    this.sourceEntityType = sourceEntityType;
    this.targetsToAdd = copyTargets(targetsToAdd, "targetsToAdd");
    this.targetsToRemove = copyTargets(targetsToRemove, "targetsToRemove");
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
        relationType, sourceIdentifier, sourceEntityType, targetsToAdd, targetsToRemove);
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
