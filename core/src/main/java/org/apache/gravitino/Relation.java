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

import org.apache.commons.lang3.tuple.Pair;

/** Represents a relation between two entities in the metadata store. */
public class Relation {
  private final Pair<NameIdentifier, Entity.EntityType> sourceVertex;
  private final Pair<NameIdentifier, Entity.EntityType> destVertex;

  /**
   * Creates a new Relation instance.
   *
   * @param sourceIdent the identifier of the source entity
   * @param sourceType the type of the source entity
   * @param destIdent the identifier of the destination entity
   * @param destType the type of the destination entity
   */
  public Relation(
      NameIdentifier sourceIdent,
      Entity.EntityType sourceType,
      NameIdentifier destIdent,
      Entity.EntityType destType) {
    this.sourceVertex = Pair.of(sourceIdent, sourceType);
    this.destVertex = Pair.of(destIdent, destType);
  }

  /**
   * Returns the source vertex of the relation.
   *
   * @return the source vertex as a pair of identifier and entity type
   */
  public Pair<NameIdentifier, Entity.EntityType> getSourceVertex() {
    return sourceVertex;
  }

  /**
   * Returns the destination vertex of the relation.
   *
   * @return the destination vertex as a pair of identifier and entity type
   */
  public Pair<NameIdentifier, Entity.EntityType> getDestVertex() {
    return destVertex;
  }

  /**
   * Returns the identifier of the source entity.
   *
   * @return the source identifier
   */
  public NameIdentifier getSourceIdent() {
    return sourceVertex.getLeft();
  }

  /**
   * Returns the type of the source entity.
   *
   * @return the source entity type
   */
  public Entity.EntityType getSourceType() {
    return sourceVertex.getRight();
  }

  /**
   * Returns the identifier of the destination entity.
   *
   * @return the destination identifier
   */
  public NameIdentifier getDestIdent() {
    return destVertex.getLeft();
  }

  /**
   * Returns the type of the destination entity.
   *
   * @return the destination entity type
   */
  public Entity.EntityType getDestType() {
    return destVertex.getRight();
  }
}
