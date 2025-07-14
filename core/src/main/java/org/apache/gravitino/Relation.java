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

import java.util.Objects;

/** Represents a relation between two entities in the metadata store. */
public class Relation {
  private final Vertex sourceVertex;
  private final Vertex destVertex;

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
    this.sourceVertex = new Vertex(sourceIdent, sourceType);
    this.destVertex = new Vertex(destIdent, destType);
  }

  /**
   * Returns the source vertex of the relation.
   *
   * @return the source vertex
   */
  public Vertex getSourceVertex() {
    return sourceVertex;
  }

  /**
   * Returns the destination vertex of the relation.
   *
   * @return the destination vertex
   */
  public Vertex getDestVertex() {
    return destVertex;
  }

  /**
   * Returns the identifier of the source entity.
   *
   * @return the source identifier
   */
  public NameIdentifier getSourceIdent() {
    return sourceVertex.getIdentifier();
  }

  /**
   * Returns the type of the source entity.
   *
   * @return the source entity type
   */
  public Entity.EntityType getSourceType() {
    return sourceVertex.getType();
  }

  /**
   * Returns the identifier of the destination entity.
   *
   * @return the destination identifier
   */
  public NameIdentifier getDestIdent() {
    return destVertex.getIdentifier();
  }

  /**
   * Returns the type of the destination entity.
   *
   * @return the destination entity type
   */
  public Entity.EntityType getDestType() {
    return destVertex.getType();
  }

  /**
   * Represents a vertex in the relation graph, which consists of an identifier and an entity type.
   */
  public static class Vertex {
    private final NameIdentifier identifier;
    private final Entity.EntityType type;

    public Vertex(NameIdentifier identifier, Entity.EntityType type) {
      this.identifier = identifier;
      this.type = type;
    }

    public NameIdentifier getIdentifier() {
      return identifier;
    }

    public Entity.EntityType getType() {
      return type;
    }

    @Override
    public String toString() {
      return "Vertex{" + "identifier=" + identifier + ", type=" + type + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Vertex)) {
        return false;
      }
      Vertex vertex = (Vertex) o;
      return Objects.equals(identifier, ((Vertex) o).identifier)
          && Objects.equals(type, vertex.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, type);
    }
  }
}
