/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client.api;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import java.util.Map;

/**
 * The client-side Catalog interface to support schema operations. If the implemented catalog has
 * schema semantics, it should implement this interface.
 */
@Evolving
public interface SupportsSchema {

  /**
   * List schemas under a namespace.
   *
   * <p>If an entity such as a table, view exists, its parent schemas must also exist and must be
   * returned by this discovery method. For example, if table a.b.t exists, this method invoked as
   * listSchemas(a) must return [a.b] in the result array
   *
   * @return An array of schema identifier under the namespace.
   * @throws NoSuchCatalogException If the catalog does not exist.
   */
  NameIdentifier[] listSchemas() throws NoSuchCatalogException;

  /**
   * Check if a schema exists.
   *
   * <p>If an entity such as a table, view exists, its parent namespaces must also exist. For
   * example, if table a.b.t exists, this method invoked as schemaExists(a.b) must return true.
   *
   * @param schemaName The name of the schema.
   * @return True if the schema exists, false otherwise.
   */
  default boolean schemaExists(String schemaName) {
    try {
      loadSchema(schemaName);
      return true;
    } catch (NoSuchSchemaException e) {
      return false;
    }
  }

  /**
   * Create a schema in the catalog.
   *
   * @param schemaName The name of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created schema.
   * @throws NoSuchCatalogException If the catalog does not exist.
   * @throws SchemaAlreadyExistsException If the schema already exists.
   */
  Schema createSchema(String schemaName, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException;

  /**
   * Load metadata properties for a schema.
   *
   * @param schemaName The name identifier of the schema.
   * @return A schema.
   * @throws NoSuchSchemaException If the schema does not exist (optional).
   */
  Schema loadSchema(String schemaName) throws NoSuchSchemaException;

  /**
   * Apply the metadata change to a schema in the catalog.
   *
   * @param schemaName The name of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered schema.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  Schema alterSchema(String schemaName, SchemaChange... changes) throws NoSuchSchemaException;

  /**
   * Drop a schema from the catalog. If cascade option is true, recursively drop all objects within
   * the schema.
   *
   * <p>If the catalog implementation does not support this operation, it may throw {@link
   * UnsupportedOperationException}.
   *
   * @param schemaName The name of the schema.
   * @param cascade If true, recursively drop all objects within the schema.
   * @return True if the schema exists and is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException If the schema is not empty and cascade is false.
   */
  boolean dropSchema(String schemaName, boolean cascade) throws NonEmptySchemaException;
}
