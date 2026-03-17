/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino;

public class Relation {
  private SupportsRelationOperations.Type type;
  private final NameIdentifier source;
  private final Entity.EntityType sourceType;
  private final NameIdentifier target;
  private final Entity.EntityType targetType;

  /**
   * Constructs a Relation with the specified type, source, target, and their entity types.
   *
   * @param type the relation type
   * @param source the source identifier
   * @param sourceType the entity type of the source
   * @param target the target identifier
   * @param targetType the entity type of the target
   */
  public Relation(
      SupportsRelationOperations.Type type,
      NameIdentifier source,
      Entity.EntityType sourceType,
      NameIdentifier target,
      Entity.EntityType targetType) {
    this.type = type;
    this.source = source;
    this.sourceType = sourceType;
    this.target = target;
    this.targetType = targetType;
  }

  /**
   * Gets the relation type.
   *
   * @return the type of the relation
   */
  public SupportsRelationOperations.Type type() {
    return type;
  }

  /**
   * Gets the source identifier.
   *
   * @return the source identifier
   */
  public NameIdentifier source() {
    return source;
  }

  /**
   * Gets the entity type of the source.
   *
   * @return the source entity type
   */
  public Entity.EntityType sourceType() {
    return sourceType;
  }

  /**
   * Gets the target identifier.
   *
   * @return the target identifier
   */
  public NameIdentifier target() {
    return target;
  }

  /**
   * Gets the entity type of the target.
   *
   * @return the target entity type
   */
  public Entity.EntityType targetType() {
    return targetType;
  }
}
