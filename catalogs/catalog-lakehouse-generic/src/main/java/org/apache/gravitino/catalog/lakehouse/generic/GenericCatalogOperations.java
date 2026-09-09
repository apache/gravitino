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
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.lakehouse.lance.LanceTableOperations;
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
import org.apache.gravitino.utils.ExceptionMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with a generic lakehouse catalog in Apache Gravitino. */
public class GenericCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GenericCatalogOperations.class);

  private final ManagedSchemaOperations schemaOps;

  private final Map<String, Supplier<ManagedTableOperations>> tableOpsCache;

  private TableLocationProvider tableLocationProvider;

  private Map<String, String> catalogProperties = Map.of();

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
    this.catalogProperties = conf == null ? Map.of() : Maps.newHashMap(conf);

    String providerName =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(conf, GenericCatalogPropertiesMetadata.TABLE_LOCATION_PROVIDER);
    this.tableLocationProvider =
        TableLocationProviderFactory.create(providerName, catalogProperties);
  }

  @Override
  public void close() throws IOException {
    tableFormatCache.cleanUp();
    if (tableLocationProvider != null) {
      tableLocationProvider.close();
    }
  }

  @Override
  public void testConnection(NameIdentifier catalogIdent) {
    throw new UnsupportedOperationException(
        "Generic catalogs do not define a catalog-level connection probe");
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

  /**
   * Drops a schema, dropping every table it contains first when cascade is set.
   *
   * <p>The cascaded tables go through {@link #dropTable(NameIdentifier)} rather than straight to
   * the table operations, so that the location of each of them is unprovisioned and its cached
   * format invalidated. A table failing to drop aborts the cascade: the tables handled so far are
   * gone, the remaining ones and the schema itself are untouched, and the call can safely be
   * retried. A {@link TableLocationProvider} failing to unprovision a location does not abort it,
   * because the table it belongs to is already gone.
   *
   * @param ident the identifier of the schema to drop
   * @param cascade whether to drop the tables contained in the schema
   * @return true if the schema was dropped, false if it did not exist
   * @throws NonEmptySchemaException if the schema contains tables and cascade is not set
   */
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

    // Drop all tables under the schema first if cascade is true. This goes through the catalog
    // level dropTable, so that the location of each table is unprovisioned and its cached format
    // invalidated.
    for (NameIdentifier tableIdent : tableIdents) {
      dropTable(tableIdent);
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
    String tableLocation =
        validateProvisionedLocation(
            tableLocationProvider.provisionTableLocation(
                TableLocationContext.builder()
                    .withTableIdentifier(ident)
                    .withTableProperties(properties)
                    .withSchema(schema)
                    .build()),
            tableLocationProvider.name(),
            ident);

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
    ManagedTableOperations tableOps = configureTableOps(tableOpsSupplier.get());

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
    return dropOrPurgeTable(ident, true /* purge */);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) throws UnsupportedOperationException {
    return dropOrPurgeTable(ident, false /* purge */);
  }

  /**
   * Drops or purges a table, and hands its location back to the {@link TableLocationProvider}
   * afterwards.
   *
   * <p>The table properties are read before the removal, because they carry the location the
   * provider has to hand back, and the unprovisioning itself happens after the removal so that a
   * provider never reclaims the storage of a table that is still there. A provider failing to
   * unprovision is logged at WARN rather than propagated: the table is already gone at that point,
   * so failing the request would report a drop that did in fact happen as unsuccessful and invite a
   * retry that cannot undo anything.
   *
   * @param ident the identifier of the table to drop
   * @param purge whether to purge the table instead of dropping it
   * @return true if the table was dropped, false if it did not exist
   */
  private boolean dropOrPurgeTable(NameIdentifier ident, boolean purge) {
    Map<String, String> tableProperties;
    try {
      tableProperties = store.get(ident, TABLE, TableEntity.class).properties();
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to load table %s before dropping it", ident), e);
    }

    // Built entirely before the drop, so that a store read failing here fails the request while
    // the table is still there, rather than after it is gone where it could only be reported as a
    // provider failure it is not.
    TableLocationContext context =
        TableLocationContext.builder()
            .withTableIdentifier(ident)
            .withTableProperties(tableProperties)
            .withSchema(loadSchema(NameIdentifier.of(ident.namespace().levels())))
            .build();

    boolean dropped = purge ? tableOps(ident).purgeTable(ident) : tableOps(ident).dropTable(ident);
    tableFormatCache.invalidate(ident);

    if (dropped) {
      try {
        tableLocationProvider.unprovisionTableLocation(context);
      } catch (Exception e) {
        LOG.warn(
            "Table {} was already {}, but table location provider '{}' failed to unprovision its "
                + "location '{}'. The storage may be leaked and needs to be reclaimed manually.",
            ident,
            purge ? "purged" : "dropped",
            tableLocationProvider.name(),
            context.tableProperties().get(Table.PROPERTY_LOCATION),
            e);
      }
    }

    return dropped;
  }

  /**
   * Returns the cache mapping a table to its format, so that tests can assert it is kept in step
   * with the tables that exist.
   *
   * @return the table format cache
   */
  @VisibleForTesting
  Cache<NameIdentifier, String> tableFormatCache() {
    return tableFormatCache;
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

      ManagedTableOperations ops = configureTableOps(tableOpsCache.get(tableFormat).get());
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
        throw ExceptionMessages.wrap("Failed to load table " + tableIdent, t);
      } else {
        throw ExceptionMessages.wrap("Unexpected exception when loading table " + tableIdent, t);
      }
    }
  }

  private ManagedTableOperations configureTableOps(ManagedTableOperations ops) {
    if (ops instanceof LanceTableOperations) {
      ((LanceTableOperations) ops).setCatalogProperties(catalogProperties);
    }

    return ops;
  }

  /**
   * Validates the location returned by a {@link TableLocationProvider} before it is stored in the
   * table properties, so that a misbehaving provider fails the table creation instead of silently
   * producing a broken location.
   *
   * <p>Only blankness is checked. The shape of the path belongs to the provider: nothing downstream
   * appends to the location, and the value is stored verbatim so that a provider unprovisioning it
   * later sees exactly the string it returned.
   *
   * @param location the location returned by the provider
   * @param providerName the name of the provider that returned it
   * @param tableIdent the identifier of the table being created
   * @return the validated location
   * @throws IllegalArgumentException if the location is blank
   */
  @VisibleForTesting
  static String validateProvisionedLocation(
      String location, String providerName, NameIdentifier tableIdent) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(location),
        "Table location provider '%s' returned a null or blank location for table %s",
        providerName,
        tableIdent);

    return location;
  }
}
