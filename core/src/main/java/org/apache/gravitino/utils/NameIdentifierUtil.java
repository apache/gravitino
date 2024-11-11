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
package org.apache.gravitino.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.IllegalNamespaceException;

/**
 * A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
 * catalog, a schema or a table. For example, "metalake1" can represent a metalake,
 * "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
 * schema.
 */
public class NameIdentifierUtil {

  private NameIdentifierUtil() {}

  /**
   * Create the metalake {@link NameIdentifierUtil} with the given name.
   *
   * @param metalake The metalake name
   * @return The created metalake {@link NameIdentifier}
   */
  public static NameIdentifier ofMetalake(String metalake) {
    return NameIdentifier.of(metalake);
  }

  /**
   * Create the catalog {@link NameIdentifierUtil} with the given metalake and catalog name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return The created catalog {@link NameIdentifier}
   */
  public static NameIdentifier ofCatalog(String metalake, String catalog) {
    return NameIdentifier.of(metalake, catalog);
  }

  /**
   * Create the schema {@link NameIdentifierUtil} with the given metalake, catalog and schema name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return The created schema {@link NameIdentifierUtil}
   */
  public static NameIdentifier ofSchema(String metalake, String catalog, String schema) {
    return NameIdentifier.of(metalake, catalog, schema);
  }

  /**
   * Create the table {@link NameIdentifier} with the given metalake, catalog, schema and table
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @return The created table {@link NameIdentifier}
   */
  public static NameIdentifier ofTable(
      String metalake, String catalog, String schema, String table) {
    return NameIdentifier.of(metalake, catalog, schema, table);
  }

  /**
   * Create the column {@link NameIdentifier} with the given metalake, catalog, schema, table and
   * column name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @param column The column name
   * @return The created column {@link NameIdentifier}
   */
  public static NameIdentifier ofColumn(
      String metalake, String catalog, String schema, String table, String column) {
    return NameIdentifier.of(metalake, catalog, schema, table, column);
  }

  /**
   * Create the fileset {@link NameIdentifier} with the given metalake, catalog, schema and fileset
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param fileset The fileset name
   * @return The created fileset {@link NameIdentifier}
   */
  public static NameIdentifier ofFileset(
      String metalake, String catalog, String schema, String fileset) {
    return NameIdentifier.of(metalake, catalog, schema, fileset);
  }

  /**
   * Create the topic {@link NameIdentifier} with the given metalake, catalog, schema and topic
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param topic The topic name
   * @return The created topic {@link NameIdentifier}
   */
  public static NameIdentifier ofTopic(
      String metalake, String catalog, String schema, String topic) {
    return NameIdentifier.of(metalake, catalog, schema, topic);
  }

  /**
   * Try to get the catalog {@link NameIdentifier} from the given {@link NameIdentifier}.
   *
   * @param ident The {@link NameIdentifier} to check.
   * @return The catalog {@link NameIdentifier}
   * @throws IllegalNameIdentifierException If the given {@link NameIdentifier} does not include
   *     catalog name
   */
  public static NameIdentifier getCatalogIdentifier(NameIdentifier ident)
      throws IllegalNameIdentifierException {
    NameIdentifier.check(
        ident.name() != null, "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && !ident.namespace().isEmpty(),
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 2) {
      throw new IllegalNameIdentifierException(
          "Cannot create a catalog NameIdentifier less than two elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1));
  }

  /**
   * Check the given {@link NameIdentifier} is a metalake identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The metalake {@link NameIdentifier} to check.
   */
  public static void checkMetalake(NameIdentifier ident) {
    // We don't have to check the name field of NameIdentifier, it's already checked when
    // creating NameIdentifier object.
    NameIdentifier.check(ident != null, "Metalake identifier must not be null");
    NamespaceUtil.checkMetalake(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a catalog identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The catalog {@link NameIdentifier} to check.
   */
  public static void checkCatalog(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Catalog identifier must not be null");
    NamespaceUtil.checkCatalog(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a schema identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The schema {@link NameIdentifier} to check.
   */
  public static void checkSchema(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Schema identifier must not be null");
    NamespaceUtil.checkSchema(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a table identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The table {@link NameIdentifier} to check.
   */
  public static void checkTable(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Table identifier must not be null");
    NamespaceUtil.checkTable(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a column identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The column {@link NameIdentifier} to check.
   */
  public static void checkColumn(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Column identifier must not be null");
    NamespaceUtil.checkColumn(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a fileset identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The fileset {@link NameIdentifier} to check.
   */
  public static void checkFileset(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Fileset identifier must not be null");
    NamespaceUtil.checkFileset(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a topic identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The topic {@link NameIdentifier} to check.
   */
  public static void checkTopic(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Topic identifier must not be null");
    NamespaceUtil.checkTopic(ident.namespace());
  }

  /**
   * Check the given condition is true. Throw an {@link IllegalNamespaceException} if it's not.
   *
   * @param expression The expression to check.
   * @param message The message to throw.
   * @param args The arguments to the message.
   */
  @FormatMethod
  public static void check(boolean expression, @FormatString String message, Object... args) {
    if (!expression) {
      throw new IllegalNamespaceException(message, args);
    }
  }

  /**
   * Convert the given {@link NameIdentifier} and {@link Entity.EntityType} to {@link
   * MetadataObject}.
   *
   * @param ident The identifier
   * @param entityType The entity type
   * @return The converted {@link MetadataObject}
   */
  public static MetadataObject toMetadataObject(
      NameIdentifier ident, Entity.EntityType entityType) {
    Preconditions.checkArgument(
        ident != null && entityType != null, "The identifier and entity type must not be null");

    Joiner dot = Joiner.on(".");

    switch (entityType) {
      case METALAKE:
        checkMetalake(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.METALAKE);

      case CATALOG:
        checkCatalog(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.CATALOG);

      case SCHEMA:
        checkSchema(ident);
        String schemaParent = ident.namespace().level(1);
        return MetadataObjects.of(schemaParent, ident.name(), MetadataObject.Type.SCHEMA);

      case TABLE:
        checkTable(ident);
        String tableParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(tableParent, ident.name(), MetadataObject.Type.TABLE);

      case COLUMN:
        checkColumn(ident);
        Namespace columnNs = ident.namespace();
        String columnParent = dot.join(columnNs.level(1), columnNs.level(2), columnNs.level(3));
        return MetadataObjects.of(columnParent, ident.name(), MetadataObject.Type.COLUMN);

      case FILESET:
        checkFileset(ident);
        String filesetParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(filesetParent, ident.name(), MetadataObject.Type.FILESET);

      case TOPIC:
        checkTopic(ident);
        String topicParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(topicParent, ident.name(), MetadataObject.Type.TOPIC);

      case ROLE:
        AuthorizationUtils.checkRole(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.ROLE);

      default:
        throw new IllegalArgumentException(
            "Entity type " + entityType + " is not supported to convert to MetadataObject");
    }
  }

  /**
   * Get the metalake name of the given {@link NameIdentifier}.
   *
   * @param identifier The name identifier of the entity
   * @return metalake name
   */
  public static String getMetalake(NameIdentifier identifier) {
    if (identifier.hasNamespace()) {
      return identifier.namespace().level(0);
    } else {
      return identifier.name();
    }
  }
}
