/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache Spark's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/SupportNamespaces.java

package org.apache.gravitino.connector;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

/**
 * The Catalog interface to support schema operations. If the implemented catalog has schema
 * semantics, it should implement this interface.
 */
@Evolving
public interface SupportsSchemas {

  /**
   * List schemas under a namespace.
   *
   * <p>If an entity such as a table, view exists, its parent schemas must also exist and must be
   * returned by this discovery method. For example, if table a.b.t exists, this method invoked as
   * listSchemas(a) must return [a.b] in the result array
   *
   * @param namespace The namespace to list.
   * @return An array of schema identifier under the namespace.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException;

  /**
   * Check if a schema exists.
   *
   * <p>If an entity such as a table, view exists, its parent namespaces must also exist. For
   * example, if table a.b.t exists, this method invoked as schemaExists(a.b) must return true.
   *
   * @param ident The name identifier of the schema.
   * @return True if the schema exists, false otherwise.
   * @throws SchemaAlreadyExistsException If loading an external schema detects an identifier owned
   *     by another existing schema; the conflict is propagated instead of returning false.
   */
  default boolean schemaExists(NameIdentifier ident) {
    try {
      loadSchema(ident);
      return true;
    } catch (NoSuchSchemaException e) {
      return false;
    }
  }

  /**
   * Create a schema in the catalog.
   *
   * @param ident The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created schema.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws SchemaAlreadyExistsException If the schema already exists.
   */
  Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException;

  /**
   * Load metadata properties for a schema.
   *
   * @param ident The name identifier of the schema.
   * @return A schema.
   * @throws NoSuchSchemaException If the schema does not exist (optional).
   * @throws SchemaAlreadyExistsException If an external schema carries the identifier of another
   *     existing schema. Remove the copied source marker before importing an independent copy.
   */
  Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException;

  /**
   * Apply the metadata change to a schema in the catalog.
   *
   * @param ident The name identifier of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered schema.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  Schema alterSchema(NameIdentifier ident, SchemaChange... changes) throws NoSuchSchemaException;

  /**
   * Drop a schema from the catalog. If cascade is true and supported by the catalog, recursively
   * drop all objects within the schema.
   *
   * <p>A catalog may refuse a non-empty schema even when cascade is requested if it cannot safely
   * restrict deletion to objects within that schema. PostgreSQL currently refuses such requests.
   *
   * <p>If the catalog implementation does not support this operation, it may throw {@link
   * UnsupportedOperationException}.
   *
   * @param ident The name identifier of the schema.
   * @param cascade Whether to request recursive deletion, subject to catalog support.
   * @return True if the schema exists and is dropped successfully, false if the schema doesn't
   *     exist.
   * @throws NonEmptySchemaException If the schema is non-empty and cascade is false, or the catalog
   *     cannot safely cascade within the schema.
   */
  boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException;
}
