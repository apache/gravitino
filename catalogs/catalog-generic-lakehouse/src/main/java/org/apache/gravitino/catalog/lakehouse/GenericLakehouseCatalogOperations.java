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
package org.apache.gravitino.catalog.lakehouse;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.hadoop.fs.Path;

/**
 * Operations for interacting with a generic lakehouse catalog in Apache Gravitino.
 *
 * <p>This catalog provides a unified interface for managing lakehouse table formats. It is designed
 * to be extensible and can support various table formats through a common interface.
 */
public class GenericLakehouseCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String SLASH = "/";

  private final ManagedSchemaOperations managedSchemaOps;

  @SuppressWarnings("unused") // todo: remove this after implementing table operations
  private Optional<Path> catalogLakehouseDir;

  /**
   * Initializes the generic lakehouse catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the generic catalog operations.
   * @param info The catalog info associated with this operation instance.
   * @param propertiesMetadata The properties metadata of generic lakehouse catalog.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    String catalogLocation =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(conf, GenericLakehouseCatalogPropertiesMetadata.LAKEHOUSE_LOCATION);
    this.catalogLakehouseLocation =
        StringUtils.isNotBlank(catalogLocation)
            ? Optional.of(catalogLocation).map(this::ensureTrailingSlash).map(Path::new)
            : Optional.empty();
  }

  public GenericLakehouseCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore());
  }

  @VisibleForTesting
  GenericLakehouseCatalogOperations(EntityStore store) {
    this.managedSchemaOps =
        new ManagedSchemaOperations() {
          @Override
          protected EntityStore store() {
            return store;
          }
        };
  }

  @Override
  public void close() {}

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    // No-op for generic lakehouse catalog.
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return managedSchemaOps.listSchemas(namespace);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return managedSchemaOps.createSchema(ident, comment, properties);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return managedSchemaOps.loadSchema(ident);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return managedSchemaOps.alterSchema(ident, changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return managedSchemaOps.dropSchema(ident, cascade);
  }

  // ==================== Table Operations (In Development) ====================
  // TODO: Implement table operations in subsequent releases
  // See: https://github.com/apache/gravitino/issues/8838
  // The table operations will delegate to format-specific implementations
  // (e.g., LanceCatalogOperations for Lance tables)

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // TODO(#8838): Implement table listing
    throw new UnsupportedOperationException(
        "Table operations are not yet implemented. "
            + "This feature is planned for a future release.");
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    // TODO(#8838): Implement table loading
    throw new UnsupportedOperationException(
        "Table operations are not yet implemented. "
            + "This feature is planned for a future release.");
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    // TODO(#8838): Implement table creation
    // This should:
    // 1. Determine table format from properties
    // 2. Delegate to format-specific implementation (e.g., LanceCatalogOperations)
    // 3. Store metadata in Gravitino entity store
    throw new UnsupportedOperationException(
        "Table operations are not yet implemented. "
            + "This feature is planned for a future release.");
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    // TODO(#8838): Implement table alteration
    throw new UnsupportedOperationException(
        "Table operations are not yet implemented. "
            + "This feature is planned for a future release.");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    // TODO(#8838): Implement table dropping
    throw new UnsupportedOperationException(
        "Table operations are not yet implemented. "
            + "This feature is planned for a future release.");
  }

  private String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }
}
