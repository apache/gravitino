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
package org.apache.gravitino.catalog.lakehouse.generic;

import static org.apache.gravitino.Entity.EntityType.TABLE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
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
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;

/** Operations for interacting with a generic lakehouse catalog in Apache Gravitino. */
public class GenericCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String SLASH = "/";

  private final ManagedSchemaOperations schemaOps;

  private final Map<String, Supplier<ManagedTableOperations>> tableOpsCache;

  private Optional<String> catalogLocation;

  private HasPropertyMetadata propertiesMetadata;

  private final Cache<NameIdentifier, String> tableFormatCache;

  private final EntityStore store;

  public GenericCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore(), GravitinoEnv.getInstance().idGenerator());
  }

  @VisibleForTesting
  GenericCatalogOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;

    this.schemaOps =
        new ManagedSchemaOperations() {
          @Override
          protected EntityStore store() {
            return store;
          }
        };

    this.tableFormatCache = CacheBuilder.newBuilder().maximumSize(1000).build();

    // Initialize all the table operations for different table formats.
    Map<String, LakehouseTableDelegator> tableDelegators =
        LakehouseTableDelegatorFactory.tableDelegators();
    tableOpsCache =
        Collections.unmodifiableMap(
            tableDelegators.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        // Lazy initialize the table operations when needed.
                        e -> {
                          LakehouseTableDelegator delegator = e.getValue();
                          return Suppliers.memoize(
                              () -> delegator.createTableOps(store, schemaOps, idGenerator));
                        })));
    if (tableOpsCache.isEmpty()) {
      throw new IllegalArgumentException("No table delegators found, this is unexpected.");
    }
  }

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    String location =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(conf, Catalog.PROPERTY_LOCATION);
    this.catalogLocation =
        StringUtils.isNotBlank(location)
            ? Optional.of(location).map(this::ensureTrailingSlash)
            : Optional.empty();
    this.propertiesMetadata = propertiesMetadata;
  }

  @Override
  public void close() {
    tableFormatCache.cleanUp();
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    // No-op for generic catalog.
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return schemaOps.listSchemas(namespace);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return schemaOps.createSchema(ident, comment, properties);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return schemaOps.loadSchema(ident);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return schemaOps.alterSchema(ident, changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    Namespace tableNs =
        Namespace.of(ident.namespace().level(0), ident.namespace().level(1), ident.name());
    NameIdentifier[] tableIdents;
    try {
      tableIdents = listTables(tableNs);
    } catch (NoSuchSchemaException e) {
      // If schema does not exist, return false.
      return false;
    }

    if (!cascade && tableIdents.length > 0) {
      throw new NonEmptySchemaException(
          "Schema %s is not empty, cannot drop it without cascade", ident);
    }

    // Drop all tables under the schema first if cascade is true.
    for (NameIdentifier tableIdent : tableIdents) {
      tableOps(tableIdent).dropTable(tableIdent);
    }

    return schemaOps.dropSchema(ident, cascade);
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    // We get the table operations from any cached table ops, since listing tables is not
    // format-specific.
    ManagedTableOperations tableOps = tableOpsCache.values().iterator().next().get();
    return tableOps.listTables(namespace);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    Table loadedTable = tableOps(ident).loadTable(ident);

    Optional<String> tableFormat =
        Optional.ofNullable(
                loadedTable.properties().getOrDefault(Table.PROPERTY_TABLE_FORMAT, null))
            .map(s -> s.toLowerCase(Locale.ROOT));
    tableFormat.ifPresent(s -> tableFormatCache.put(ident, s));

    return loadedTable;
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

    String format = properties.getOrDefault(Table.PROPERTY_TABLE_FORMAT, null);
    Preconditions.checkArgument(
        format != null, "Table format must be specified in table properties");
    format = format.toLowerCase(Locale.ROOT);

    Map<String, String> newProperties = Maps.newHashMap(properties);
    newProperties.put(Table.PROPERTY_LOCATION, tableLocation);
    newProperties.put(Table.PROPERTY_TABLE_FORMAT, format);

    // Get the table operations for the specified table format.
    Supplier<ManagedTableOperations> tableOpsSupplier = tableOpsCache.get(format);
    Preconditions.checkArgument(tableOpsSupplier != null, "Unsupported table format: %s", format);
    ManagedTableOperations tableOps = tableOpsSupplier.get();

    Table createdTable =
        tableOps.createTable(
            ident, columns, comment, newProperties, partitions, distribution, sortOrders, indexes);
    // Cache the table format for future use.
    tableFormatCache.put(ident, format);
    return createdTable;
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    Table alteredTable = tableOps(ident).alterTable(ident, changes);

    boolean isRenameChange =
        Arrays.stream(changes).anyMatch(c -> c instanceof TableChange.RenameTable);
    if (isRenameChange) {
      tableFormatCache.invalidate(ident);
    }

    return alteredTable;
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    boolean purged = tableOps(ident).purgeTable(ident);
    tableFormatCache.invalidate(ident);
    return purged;
  }

  @Override
  public boolean dropTable(NameIdentifier ident) throws UnsupportedOperationException {
    boolean dropped = tableOps(ident).dropTable(ident);
    tableFormatCache.invalidate(ident);
    return dropped;
  }

  private String calculateTableLocation(
      Schema schema, NameIdentifier tableIdent, Map<String, String> tableProperties) {
    String tableLocation =
        (String)
            propertiesMetadata
                .tablePropertiesMetadata()
                .getOrDefault(tableProperties, Table.PROPERTY_LOCATION);
    if (StringUtils.isNotBlank(tableLocation)) {
      return ensureTrailingSlash(tableLocation);
    }

    String schemaLocation =
        schema.properties() == null ? null : schema.properties().get(Schema.PROPERTY_LOCATION);

    // If we do not set location in table properties, and schema location is set, use schema
    // location as the base path.
    if (StringUtils.isNotBlank(schemaLocation)) {
      return ensureTrailingSlash(schemaLocation) + tableIdent.name() + SLASH;
    }

    // If the schema location is not set, use catalog lakehouse dir as the base path. Or else, throw
    // an exception.
    if (catalogLocation.isEmpty()) {
      throw new IllegalArgumentException(
          "'location' property is neither set in table properties "
              + "nor in schema properties, and no location is set in catalog properties either. "
              + "Please set the 'location' in either of them to create the table "
              + tableIdent);
    }

    return ensureTrailingSlash(catalogLocation.get())
        + tableIdent.namespace().level(2)
        + SLASH
        + tableIdent.name()
        + SLASH;
  }

  private String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }

  private ManagedTableOperations tableOps(NameIdentifier tableIdent) {
    try {
      String tableFormat =
          tableFormatCache.get(
              tableIdent,
              () -> {
                TableEntity table = store.get(tableIdent, TABLE, TableEntity.class);
                String format = table.properties().getOrDefault(Table.PROPERTY_TABLE_FORMAT, null);
                Preconditions.checkArgument(
                    format != null, "Table format for %s is null, this is unexpected", tableIdent);

                return format.toLowerCase(Locale.ROOT);
              });

      ManagedTableOperations ops = tableOpsCache.get(tableFormat).get();
      Preconditions.checkArgument(
          ops != null, "No table operations found for table format %s", tableFormat);
      return ops;

    } catch (Exception e) {
      Throwable t = e.getCause();

      if (t instanceof NoSuchEntityException) {
        throw new NoSuchTableException("Table %s does not exist", tableIdent);
      } else if (t instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) t;
      } else if (t instanceof IOException) {
        throw new RuntimeException(
            String.format("Failed to load table %s: %s", tableIdent, t.getMessage()), t);
      } else {
        throw new RuntimeException(
            String.format("Unexpected exception when loading table %s", tableIdent), t);
      }
    }
  }
}
