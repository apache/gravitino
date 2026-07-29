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
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Describes a relation lookup from an anchor entity. The anchor may be either endpoint of the
 * relation edge, and the returned entities come from the opposite endpoint.
 *
 * <p>For reverse lookups, callers still use the same relation type. For example, querying all
 * metadata objects that have a specific policy uses {@link
 * SupportsRelationOperations.Type#POLICY_METADATA_OBJECT_REL} with the policy as the anchor entity,
 * and querying all metadata objects that have a specific tag uses {@link
 * SupportsRelationOperations.Type#TAG_METADATA_OBJECT_REL} with the tag as the anchor entity.
 *
 * <p>The optional relation value is an exact string value carried by the relation edge, such as a
 * tag assignment value. A null value means the query should not filter by relation value.
 */
public final class RelationQuery {

  private final SupportsRelationOperations.Type relationType;
  private final NameIdentifier anchorIdentifier;
  private final Entity.EntityType anchorEntityType;
  private final boolean allFields;
  @Nullable private final String relationValue;

  private RelationQuery(
      SupportsRelationOperations.Type relationType,
      NameIdentifier anchorIdentifier,
      Entity.EntityType anchorEntityType,
      boolean allFields,
      @Nullable String relationValue) {
    this.relationType = relationType;
    this.anchorIdentifier = anchorIdentifier;
    this.anchorEntityType = anchorEntityType;
    this.allFields = allFields;
    this.relationValue = relationValue;
  }

  /**
   * Creates a relation query.
   *
   * @param relationType The type of relation.
   * @param anchorIdentifier The anchor entity identifier.
   * @param anchorEntityType The entity type that {@code anchorIdentifier} represents.
   * @param allFields Whether to fetch all fields.
   * @param relationValue Optional exact string value carried by the relation edge.
   * @return A relation query.
   */
  public static RelationQuery of(
      SupportsRelationOperations.Type relationType,
      NameIdentifier anchorIdentifier,
      Entity.EntityType anchorEntityType,
      boolean allFields,
      @Nullable String relationValue) {
    Preconditions.checkArgument(relationType != null, "relationType must not be null");
    Preconditions.checkArgument(anchorIdentifier != null, "anchorIdentifier must not be null");
    Preconditions.checkArgument(anchorEntityType != null, "anchorEntityType must not be null");
    return new RelationQuery(
        relationType, anchorIdentifier, anchorEntityType, allFields, relationValue);
  }

  /**
   * @return The type of relation.
   */
  public SupportsRelationOperations.Type relationType() {
    return relationType;
  }

  /**
   * @return The anchor entity identifier.
   */
  public NameIdentifier anchorIdentifier() {
    return anchorIdentifier;
  }

  /**
   * @return The entity type that {@link #anchorIdentifier()} represents.
   */
  public Entity.EntityType anchorEntityType() {
    return anchorEntityType;
  }

  /**
   * @return Whether to fetch all fields.
   */
  public boolean allFields() {
    return allFields;
  }

  /**
   * @return The optional exact string value carried by the relation edge.
   */
  public Optional<String> relationValue() {
    return Optional.ofNullable(relationValue);
  }
}
