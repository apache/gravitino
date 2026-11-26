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
package org.apache.gravitino.paimon.service;

import java.util.List;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Provider;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.glassfish.jersey.server.CloseableService;

/** Resolves a Paimon catalog and holds it until Jersey finishes the request. */
public class PaimonCatalogRequest {
  private final PaimonCatalogWrapperManager manager;
  private final Provider<CloseableService> closeables;
  private final PaimonAuthorization authorization;
  private final PaimonMetadataHooks metadataHooks;
  private final String metalake;

  /**
   * Creates a resolver for the Jersey request lifecycle.
   *
   * @param manager the backend catalog manager
   * @param closeables the current request's closeable service
   * @param authorization metadata authorization policy
   * @param metadataHooks auxiliary metadata ownership hooks
   */
  @Inject
  public PaimonCatalogRequest(
      PaimonCatalogWrapperManager manager,
      Provider<CloseableService> closeables,
      PaimonAuthorization authorization,
      PaimonMetadataHooks metadataHooks) {
    this.manager = manager;
    this.closeables = closeables;
    this.authorization = authorization;
    this.metadataHooks = metadataHooks;
    this.metalake = authorization.metalake();
  }

  /**
   * Acquires a catalog and registers it for cleanup when the request completes.
   *
   * @param name the catalog name
   * @return the catalog lease
   */
  public CatalogLease acquire(String name) {
    authorization.require(
        authorization.catalog(
            name, AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION));
    CloseableService closeableService = closeables.get();
    CatalogLease lease = manager.acquire(name);
    if (!closeableService.add(lease)) {
      lease.close();
      throw new IllegalStateException("Could not register the Paimon catalog for request cleanup.");
    }
    return lease;
  }

  /**
   * Returns the catalog for the request.
   *
   * @param name the catalog name
   * @return the backend catalog
   */
  public PaimonCatalogWrapper getCatalogWrapper(String name) {
    return acquire(name).wrapper();
  }

  /**
   * Resolves a table for reading, allowing authorized creation/existence probes of missing tables.
   *
   * @param catalog registered catalog name
   * @param identifier native table identifier
   * @return backend when table metadata may be read
   */
  public PaimonCatalogWrapper getTableWrapper(String catalog, Identifier identifier) {
    if (!authorization.table(
        catalog,
        identifier.getDatabaseName(),
        identifier.getTableName(),
        AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION)) {
      requireTable(
          catalog,
          identifier,
          AuthorizationExpressionConstants.PROBE_TABLE_LIKE_AUTHORIZATION_EXPRESSION);
      // A missing resource keeps its 404. An existing resource requires metadata read access.
      getCatalogWrapper(catalog).getFileTable(identifier);
      throw new ForbiddenException("Table metadata read access is required.");
    }
    return getCatalogWrapper(catalog);
  }

  /**
   * Resolves a schema for reading, allowing authorized creation probes of missing schemas.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @return backend when schema metadata may be read
   */
  public PaimonCatalogWrapper getSchemaWrapper(String catalog, String database) {
    if (!authorization.schema(
        catalog, database, AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION)) {
      requireSchema(
          catalog,
          database,
          AuthorizationExpressionConstants.PROBE_SCHEMA_AUTHORIZATION_EXPRESSION);
      getCatalogWrapper(catalog).getDatabase(database);
      throw new ForbiddenException("Schema metadata read access is required.");
    }
    return getCatalogWrapper(catalog);
  }

  /**
   * Checks schema privileges for the current request.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @param expression required Gravitino authorization expression
   */
  public void requireSchema(String catalog, String database, String expression) {
    authorization.require(authorization.schema(catalog, database, expression));
  }

  /**
   * Checks table privileges using the base table for branch requests.
   *
   * @param catalog registered catalog name
   * @param identifier Paimon table identifier
   * @param expression required Gravitino authorization expression
   */
  public void requireTable(String catalog, Identifier identifier, String expression) {
    authorization.require(
        authorization.table(
            catalog, identifier.getDatabaseName(), identifier.getTableName(), expression));
  }

  /**
   * Checks the privileges for catalog-wide storage credential vending.
   *
   * @param catalog registered catalog name
   */
  public void requireCredentials(String catalog) {
    authorization.require(
        authorization.catalog(
            catalog,
            AuthorizationExpressionConstants.CATALOG_STATIC_CREDENTIALS_AUTHORIZATION_EXPRESSION));
  }

  /**
   * Filters tables with Gravitino's table visibility expression.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @param table base table name
   * @return whether the current principal may see the table
   */
  public boolean canReadTable(String catalog, String database, String table) {
    return authorization.table(
        catalog,
        database,
        table,
        AuthorizationExpressionConstants.LIST_TABLE_LIKE_AUTHORIZATION_EXPRESSION);
  }

  /**
   * Filters schemas with Gravitino's schema visibility expression.
   *
   * @param catalog registered catalog name
   * @param database schema name
   * @return whether the current principal may see the schema
   */
  public boolean canReadSchema(String catalog, String database) {
    return authorization.schema(
        catalog, database, AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION);
  }
  /**
   * Applies a schema mutation and its Gravitino metadata hooks.
   *
   * @param <T> native result type
   * @return native operation result
   * @param change metadata effect
   * @param catalog registered catalog name
   * @param database schema name
   * @param mutation native mutation
   */
  public <T> T mutateSchema(
      PaimonMetadataHooks.Change change, String catalog, String database, Supplier<T> mutation) {
    return metadataHooks.mutate(change, NameIdentifier.of(metalake, catalog, database), mutation);
  }

  /**
   * Applies a table mutation and its Gravitino metadata hooks.
   *
   * @param <T> native result type
   * @return native operation result
   * @param change metadata effect
   * @param catalog registered catalog name
   * @param identifier native table identifier
   * @param mutation native mutation
   */
  public <T> T mutateTable(
      PaimonMetadataHooks.Change change,
      String catalog,
      Identifier identifier,
      Supplier<T> mutation) {
    if (identifier.getBranchName() != null
        && !"main".equalsIgnoreCase(identifier.getBranchName())) {
      return mutation.get();
    }
    return metadataHooks.mutate(
        change,
        NameIdentifier.of(
            metalake, catalog, identifier.getDatabaseName(), identifier.getTableName()),
        mutation);
  }

  /**
   * Validates and creates a native table with its registered metadata.
   *
   * @param catalog registered catalog name
   * @param identifier native identifier
   * @param wrapper leased native catalog
   * @param schema requested schema
   */
  public void createTable(
      String catalog, Identifier identifier, PaimonCatalogWrapper wrapper, Schema schema) {
    metadataHooks.validateCreateTable(schema);
    mutateTable(
        PaimonMetadataHooks.Change.CREATE_TABLE,
        catalog,
        identifier,
        () -> {
          wrapper.createTable(identifier, schema);
          return null;
        });
  }

  /**
   * Applies schema changes and preserves registered column identities using Paimon field IDs.
   *
   * @param catalog registered catalog name
   * @param identifier native table identifier
   * @param wrapper leased native catalog
   * @param changes native schema changes
   */
  public void alterTable(
      String catalog,
      Identifier identifier,
      PaimonCatalogWrapper wrapper,
      List<SchemaChange> changes) {
    metadataHooks.validateTableChanges(changes);
    if (identifier.getBranchName() != null
        && !"main".equalsIgnoreCase(identifier.getBranchName())) {
      wrapper.alterTable(identifier, changes);
      return;
    }
    metadataHooks.alterTable(
        NameIdentifier.of(
            metalake, catalog, identifier.getDatabaseName(), identifier.getTableName()),
        () -> wrapper.getFileTable(identifier).schema().fields(),
        () -> wrapper.alterTable(identifier, changes));
  }

  /**
   * Applies a rename and preserves Gravitino-only table metadata.
   *
   * @param catalog registered catalog name
   * @param source old native identifier
   * @param destination new native identifier
   * @param mutation native rename
   */
  public void renameTable(
      String catalog, Identifier source, Identifier destination, Runnable mutation) {
    metadataHooks.rename(
        NameIdentifier.of(metalake, catalog, source.getDatabaseName(), source.getTableName()),
        NameIdentifier.of(
            metalake, catalog, destination.getDatabaseName(), destination.getTableName()),
        mutation);
  }
}
