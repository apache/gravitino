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

package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import java.util.Map;

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
   * Drop a schema from the catalog. If cascade option is true, recursively drop all objects within
   * the schema.
   *
   * <p>If the catalog implementation does not support this operation, it may throw {@link
   * UnsupportedOperationException}.
   *
   * @param ident The name identifier of the schema.
   * @param cascade If true, recursively drop all objects within the schema.
   * @return True if the schema exists and is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException If the schema is not empty and cascade is false.
   */
  boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException;
}
