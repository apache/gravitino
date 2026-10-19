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
import org.apache.gravitino.lance.common.utils.LancePropertiesUtils;
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

/** Operations for interacting with a generic lakehouse catalog in Apache Gravitino. */
public class GenericCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final String SLASH = "/";

  private final ManagedSchemaOperations schemaOps;

  private final Map<String, Supplier<ManagedTableOperations>> tableOpsCache;

  private Optional<String> catalogLocation;

  private Map<String, String> catalogProperties = Map.of();

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
    this.catalogProperties = conf == null ? Map.of() : Maps.newHashMap(conf);
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
    ManagedTableOperations tableOps = configureTableOps(tableOpsSupplier.get());

<<<<<<< HEAD
=======
    // The provider is consulted only once the request is known to be one this catalog can serve.
    // A provider that allocates real storage has no compensating callback, so every check that can
    // be made before asking it for a location is one reservation it does not have to reclaim.
    //
    // It is consulted even when the request carries its own `location`. The catalog used to skip
    // it there, which read as respect for the caller's choice but left the provider unable to see,
    // let alone refuse, a path outside the policy it exists to enforce -- the very bypass this SPI
    // is meant to close. The supplied value is visible as the `location` entry of the context's
    // table properties, and the built-in provider returns it verbatim as its first branch, so a
    // catalog on the built-in provider is unaffected. What an allocating provider does with it is
    // the provider's decision: honour it, rewrite it, or reject the creation.
    properties = LancePropertiesUtils.normalizeTableProperties(properties);
    String tableLocation = provisionTableLocation(ident, properties, schema);

    Map<String, String> newProperties = Maps.newHashMap(properties);
    newProperties.put(Table.PROPERTY_LOCATION, tableLocation);
    newProperties.put(Table.PROPERTY_TABLE_FORMAT, format);

>>>>>>> f742f1100 ([#13931] fix(lance): preserve externally owned registered datasets (#13932))
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
<<<<<<< HEAD
    boolean dropped = tableOps(ident).dropTable(ident);
    tableFormatCache.invalidate(ident);
=======
    return dropOrPurgeTable(ident, false /* purge */);
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
    return dropOrPurgeTable(
        ident, purge, loadSchema(NameIdentifier.of(ident.namespace().levels())));
  }

  /**
   * Drops or purges a table, resolving its parent schema through the given supplier.
   *
   * <p>The schema is passed in rather than loaded here so that a cascading schema drop, where every
   * table shares one parent, loads it once instead of once per table. It stays eager: the context
   * is built in full before the table is removed, so that a store read failing fails the request
   * while the table is still there, rather than from inside a callback where it could only be
   * reported as a provider failure it is not.
   *
   * @param ident the identifier of the table to drop
   * @param purge whether to purge the table instead of dropping it
   * @param schema the table's parent schema
   * @return true if the table was dropped, false if it did not exist
   */
  private boolean dropOrPurgeTable(NameIdentifier ident, boolean purge, Schema schema) {
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
            .withSchema(schema)
            .withCatalogProperties(catalogProperties)
            .build();

    // The properties just read are handed on rather than left to be read again: resolving the
    // table format is a second store read for the very same entity whenever the format cache is
    // cold, which for a drop it usually is.
    ManagedTableOperations tableOps = tableOps(ident, tableProperties);
    boolean dropped;
    try {
      dropped = purge ? tableOps.purgeTable(ident) : tableOps.dropTable(ident);
    } catch (RuntimeException e) {
      // A format that fails partway through a removal may already have deleted the metadata --
      // LanceTableOperations removes the entity first and deletes the dataset second, and wraps a
      // failure of the second step. The table can be gone with its location still allocated, and
      // the caller sees only the format's exception, so the location is named here or nowhere. It
      // is not unprovisioned: the format may equally have failed before touching storage, and
      // handing back a location that still has data under it is the unrecoverable direction, the
      // same reasoning as limitation (2) on a failed creation.
      LOG.warn(
          "Table format failed to {} table {}. If the metadata was already removed, its location "
              + "'{}' is not handed back to table location provider '{}' and may be leaked.",
          purge ? "purge" : "drop",
          ident,
          context.tableProperties().get(Table.PROPERTY_LOCATION),
          tableLocationProvider.name(),
          e);
      throw e;
    } finally {
      // In a finally because a format that threw may still have removed the metadata, and a cache
      // entry for a table that no longer exists is a stale format for whatever is created next
      // under the same name.
      tableFormatCache.invalidate(ident);
    }

    // The rule is "hand the location back once the data under it is gone", and `external` only
    // approximates it on a drop: the table formats leave an external dataset in place there, so
    // asking for the location back would invite the provider to delete exactly what the catalog
    // just promised not to touch. A purge is the opposite request --
    // LanceTableOperations#purgeTable
    // deletes the external dataset its dropTable leaves alone -- so the location is handed back
    // there whichever way `external` reads.
    //
    // The decision is made here rather than handed to the provider. By the time this runs the data
    // is gone in all three cases that reach it, so there is nothing left for a provider to decide
    // differently; and the case that is skipped is the one where a provider reclaiming by path
    // would delete a user's data irrecoverably.
    // Read the same way the table formats and the property metadata read it: a catalog that
    // disagreed with the format about a value like `external=yes` would skip reclaiming a location
    // the format had just deleted the data under.
    // Use the same effective ownership as the format, including legacy Lance registrations.
    boolean external = LancePropertiesUtils.isExternalTable(context.tableProperties());
    if (dropped && (purge || !external)) {
      TableLocationProvider provider = tableLocationProvider;
      try {
        provider.unprovisionTableLocation(context);
      } catch (Exception e) {
        LOG.warn(
            "Table {} was already {}, but table location provider '{}' failed to unprovision its "
                + "location '{}'. The storage may be leaked and needs to be reclaimed manually.",
            ident,
            purge ? "purged" : "dropped",
            provider.name(),
            context.tableProperties().get(Table.PROPERTY_LOCATION),
            e);
      }
    } else if (dropped) {
      LOG.debug(
          "Table {} is external and was dropped rather than purged, so its location '{}' is left "
              + "alone rather than handed back to table location provider '{}': the data is still "
              + "there and the catalog does not own it.",
          ident,
          context.tableProperties().get(Table.PROPERTY_LOCATION),
          tableLocationProvider.name());
    }

>>>>>>> f742f1100 ([#13931] fix(lance): preserve externally owned registered datasets (#13932))
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
}
