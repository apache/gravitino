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
 * Represents the immutable target endpoint of a relation edge in a relation update. The source
 * endpoint is supplied by {@link RelationUpdate}.
 *
 * <p>The optional relation value is metadata stored on the edge itself rather than on either
 * endpoint entity. For example, a tag assignment value belongs to the tag-to-metadata-object
 * relation, not to the tag definition or metadata object.
 */
public final class RelationEdgeTarget {

  private final NameIdentifier nameIdentifier;
  private final Entity.EntityType entityType;
  @Nullable private final String relationValue;

  private RelationEdgeTarget(
      NameIdentifier nameIdentifier, Entity.EntityType entityType, @Nullable String relationValue) {
    this.nameIdentifier = nameIdentifier;
    this.entityType = entityType;
    this.relationValue = relationValue;
  }

  /**
   * Creates a relation edge target.
   *
   * @param nameIdentifier The target entity identifier.
   * @param entityType The target entity type.
   * @param relationValue Optional string value carried by the relation edge.
   * @return A relation edge target.
   */
  public static RelationEdgeTarget of(
      NameIdentifier nameIdentifier, Entity.EntityType entityType, @Nullable String relationValue) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(entityType != null, "entityType must not be null");
    return new RelationEdgeTarget(nameIdentifier, entityType, relationValue);
  }

  /**
   * @return The target entity identifier.
   */
  public NameIdentifier nameIdentifier() {
    return nameIdentifier;
  }

  /**
   * @return The target entity type.
   */
  public Entity.EntityType entityType() {
    return entityType;
  }

  /**
   * @return The optional string value carried by the relation edge.
   */
  public Optional<String> relationValue() {
    return Optional.ofNullable(relationValue);
  }
}
