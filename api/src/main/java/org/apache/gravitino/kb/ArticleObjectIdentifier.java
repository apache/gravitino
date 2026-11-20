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

package org.apache.gravitino.kb;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * Identifies a metadata object bound to a Knowledge Base Article by its type, ordered namespace
 * components and name.
 *
 * <p>The components are carried as exact strings, never joined into a dotted name: a column named
 * {@code total.amount} stays one name component through REST, the clients and MCP. The server
 * validates the component count for the target type and resolves the components directly to the
 * target's stable ID.
 */
@Evolving
public final class ArticleObjectIdentifier {

  private final MetadataObject.Type type;
  private final List<String> namespace;
  private final String name;

  /**
   * Creates an object identifier.
   *
   * @param type the metadata object type
   * @param namespace the ordered namespace components; empty for a catalog
   * @param name the object name
   * @return the identifier
   */
  public static ArticleObjectIdentifier of(
      MetadataObject.Type type, List<String> namespace, String name) {
    return new ArticleObjectIdentifier(type, namespace, name);
  }

  /**
   * Creates an object identifier.
   *
   * @param type the metadata object type
   * @param namespace the ordered namespace components; empty for a catalog
   * @param name the object name
   * @return the identifier
   */
  public static ArticleObjectIdentifier of(
      MetadataObject.Type type, String[] namespace, String name) {
    return new ArticleObjectIdentifier(
        type, namespace == null ? null : Arrays.asList(namespace), name);
  }

  private ArticleObjectIdentifier(MetadataObject.Type type, List<String> namespace, String name) {
    Preconditions.checkArgument(type != null, "Bound object type must not be null");
    Preconditions.checkArgument(namespace != null, "Bound object namespace must not be null");
    Preconditions.checkArgument(
        name != null && !name.trim().isEmpty(), "Bound object name must not be blank");
    int depth;
    switch (type) {
      case CATALOG:
        depth = 0;
        break;
      case SCHEMA:
        depth = 1;
        break;
      case TABLE:
      case VIEW:
      case FILESET:
      case TOPIC:
      case MODEL:
      case SEMANTIC_MODEL:
        depth = 2;
        break;
      case COLUMN:
        depth = 3;
        break;
      default:
        throw new IllegalArgumentException("Unsupported article binding type: " + type);
    }
    Preconditions.checkArgument(
        namespace.size() == depth, "%s requires %s namespace components", type, depth);
    for (String component : namespace) {
      Preconditions.checkArgument(
          component != null && !component.trim().isEmpty(), "Blank namespace component");
    }
    this.type = type;
    this.namespace = ImmutableList.copyOf(namespace);
    this.name = name;
  }

  /**
   * @return the metadata object type
   */
  public MetadataObject.Type type() {
    return type;
  }

  /**
   * @return the ordered namespace components; empty for a catalog
   */
  public List<String> namespace() {
    return namespace;
  }

  /**
   * @return the object name
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArticleObjectIdentifier that = (ArticleObjectIdentifier) o;
    return type == that.type
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, namespace, name);
  }

  @Override
  public String toString() {
    return "ArticleObjectIdentifier{type="
        + type
        + ", namespace="
        + namespace
        + ", name="
        + name
        + "}";
  }
}
