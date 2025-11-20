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

import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.apache.gravitino.catalog.lakehouse.GenericLakehouseTablePropertiesMetadata.LOCATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.lakehouse.lance.LanceCatalogOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.GenericTableEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operations for interacting with a generic lakehouse catalog in Apache Gravitino.
 *
 * <p>This catalog provides a unified interface for managing lakehouse table formats. It is designed
 * to be extensible and can support various table formats through a common interface.
 */
public class GenericLakehouseCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog {
  private static final Logger LOG =
      LoggerFactory.getLogger(GenericLakehouseCatalogOperations.class);

  private static final String SLASH = "/";

  private final ManagedSchemaOperations managedSchemaOps;

  @SuppressWarnings("unused") // todo: remove this after implementing table operations
  private Optional<Path> catalogLakehouseLocation;

  private static final Map<String, LakehouseCatalogOperations> SUPPORTED_FORMATS =
      Maps.newHashMap();

  private Map<String, String> catalogConfig;
  private CatalogInfo catalogInfo;
  private HasPropertyMetadata propertiesMetadata;

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
    this.catalogConfig = conf;
    this.catalogInfo = info;
    this.propertiesMetadata = propertiesMetadata;
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
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    NameIdentifier identifier = NameIdentifier.of(namespace.levels());
    try {
      store.get(identifier, Entity.EntityType.SCHEMA, SchemaEntity.class);
    } catch (NoSuchTableException e) {
      throw new NoSuchEntityException(e, "Schema %s does not exist", namespace);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to get schema " + identifier);
    }

    try {
      List<GenericTableEntity> tableEntityList =
          store.list(namespace, GenericTableEntity.class, TABLE);
      return tableEntityList.stream()
          .map(e -> NameIdentifier.of(namespace, e.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list tables under schema " + namespace, e);
    }
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
    Schema schema = loadSchema(NameIdentifier.of(ident.namespace().levels()));
    String tableLocation = calculateTableLocation(schema, ident, properties);
    Map<String, String> tableStorageProps = calculateTableStorageProps(schema, properties);

    Map<String, String> newProperties = Maps.newHashMap(properties);
    newProperties.put(LOCATION, tableLocation);
    newProperties.putAll(tableStorageProps);

    String format = properties.getOrDefault("format", "lance");
    LakehouseCatalogOperations lakehouseCatalogOperations =
        SUPPORTED_FORMATS.compute(
            format,
            (k, v) ->
                v == null
                    ? createLakehouseCatalogOperations(
                        format, properties, catalogInfo, propertiesMetadata)
                    : v);

    return lakehouseCatalogOperations.createTable(
        ident, columns, comment, newProperties, partitions, distribution, sortOrders, indexes);
  }

  private String calculateTableLocation(
      Schema schema, NameIdentifier tableIdent, Map<String, String> tableProperties) {
    String tableLocation = tableProperties.get(LOCATION);
    if (StringUtils.isNotBlank(tableLocation)) {
      return ensureTrailingSlash(tableLocation);
    }

    String schemaLocation = schema.properties() == null ? null : schema.properties().get(LOCATION);

    // If we do not set location in table properties, and schema location is set, use schema
    // location
    // as the base path.
    if (StringUtils.isNotBlank(schemaLocation)) {
      return ensureTrailingSlash(schemaLocation) + tableIdent.name() + SLASH;
    }

    // If the schema location is not set, use catalog lakehouse dir as the base path. Or else, throw
    // an exception.
    if (catalogLakehouseLocation.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "No location specified for table %s, you need to set location either in catalog, schema, or table properties",
              tableIdent));
    }

    String catalogLakehousePath = catalogLakehouseLocation.get().toString();
    String[] nsLevels = tableIdent.namespace().levels();
    String schemaName = nsLevels[nsLevels.length - 1];
    return ensureTrailingSlash(catalogLakehousePath)
        + schemaName
        + SLASH
        + tableIdent.name()
        + SLASH;
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    Namespace namespace = ident.namespace();
    try {
      GenericTableEntity tableEntity =
          store.get(ident, Entity.EntityType.TABLE, GenericTableEntity.class);
      Map<String, String> tableProperties = tableEntity.getProperties();
      String format = tableProperties.getOrDefault("format", "lance");
      LakehouseCatalogOperations lakehouseCatalogOperations =
          SUPPORTED_FORMATS.compute(
              format,
              (k, v) ->
                  v == null
                      ? createLakehouseCatalogOperations(
                          format, tableProperties, catalogInfo, propertiesMetadata)
                      : v);
      return lakehouseCatalogOperations.alterTable(ident, changes);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list tables under schema " + namespace, e);
    }
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    GenericTableEntity tableEntity;
    try {
      tableEntity = store.get(ident, Entity.EntityType.TABLE, GenericTableEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Table {} does not exist, skip dropping.", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to get table " + ident);
    }

    Map<String, String> tableProperties = tableEntity.getProperties();
    String format = tableProperties.getOrDefault("format", "lance");
    LakehouseCatalogOperations lakehouseCatalogOperations =
        SUPPORTED_FORMATS.compute(
            format,
            (k, v) ->
                v == null
                    ? createLakehouseCatalogOperations(
                        format, tableProperties, catalogInfo, propertiesMetadata)
                    : v);
    return lakehouseCatalogOperations.dropTable(ident);
  }

  private String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }

  private LakehouseCatalogOperations createLakehouseCatalogOperations(
      String format,
      Map<String, String> properties,
      CatalogInfo catalogInfo,
      HasPropertyMetadata propertiesMetadata) {
    LakehouseCatalogOperations operations;
    switch (format.toLowerCase()) {
      case "lance":
        operations = new LanceCatalogOperations();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported lakehouse format: " + format);
    }

    operations.initialize(properties, catalogInfo, propertiesMetadata);
    return operations;
  }

  /**
   * Calculate the table storage properties by merging catalog config, schema properties and table
   * properties. The precedence is: table properties > schema properties > catalog config.
   *
   * @param schema The schema of the table.
   * @param tableProps The table properties.
   * @return The merged table storage properties.
   */
  private Map<String, String> calculateTableStorageProps(
      Schema schema, Map<String, String> tableProps) {
    Map<String, String> storageProps = getLanceTableStorageOptions(catalogConfig);
    storageProps.putAll(getLanceTableStorageOptions(schema.properties()));
    storageProps.putAll(getLanceTableStorageOptions(tableProps));
    return storageProps;
  }

  private Map<String, String> getLanceTableStorageOptions(Map<String, String> properties) {
    if (MapUtils.isEmpty(properties)) {
      return Maps.newHashMap();
    }
    return properties.entrySet().stream()
        .filter(
            e ->
                e.getKey()
                    .startsWith(
                        GenericLakehouseTablePropertiesMetadata.LANCE_TABLE_STORAGE_OPTION_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
