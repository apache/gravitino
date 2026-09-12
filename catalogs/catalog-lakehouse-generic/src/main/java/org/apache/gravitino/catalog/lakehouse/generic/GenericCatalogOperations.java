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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

  private volatile TableLocationProvider tableLocationProvider;

  private volatile Map<String, String> catalogProperties = Map.of();

  private final AtomicBoolean closed = new AtomicBoolean();

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
    // A defensive copy that tolerates null values, for the same reason as
    // TableLocationContext.Builder#withTableProperties: nothing upstream rejects a catalog
    // property whose value is null, and ImmutableMap.copyOf would turn one into a
    // NullPointerException. Here it would fail the creation of the whole catalog rather than a
    // single request.
    this.catalogProperties =
        conf == null ? Map.of() : Collections.unmodifiableMap(Maps.newHashMap(conf));

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

    // A flag rather than clearing the field: closing the catalog twice must not close the provider
    // twice, but a drop in flight while the catalog shuts down must not meet a null provider
    // either. The contract asks a provider to tolerate close after a failed initialize, not
    // repeated closes from its owner.
    TableLocationProvider provider = tableLocationProvider;
    if (provider != null && closed.compareAndSet(false, true)) {
      provider.close();
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

    // Drop all tables under the schema first if cascade is true. This goes through the same path
    // as the catalog level dropTable, so that the location of each table is unprovisioned and its
    // cached format invalidated. The schema is resolved once for the whole cascade rather than
    // once per table: every table here has the same parent, and the provider may not ask for it at
    // all.
    Schema cascadedSchema = loadSchema(ident);
    for (NameIdentifier tableIdent : tableIdents) {
      dropOrPurgeTable(tableIdent, false /* purge */, cascadedSchema);
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

    String format = properties.getOrDefault(Table.PROPERTY_TABLE_FORMAT, null);
    Preconditions.checkArgument(
        format != null, "Table format must be specified in table properties");
    format = format.toLowerCase(Locale.ROOT);

    // Get the table operations for the specified table format.
    Supplier<ManagedTableOperations> tableOpsSupplier = tableOpsCache.get(format);
    Preconditions.checkArgument(tableOpsSupplier != null, "Unsupported table format: %s", format);
    ManagedTableOperations tableOps = configureTableOps(tableOpsSupplier.get());

    // The provider is consulted only once the request is known to be one this catalog can serve,
    // and only when the catalog is the one choosing the location. A provider that allocates real
    // storage has no compensating callback, so every check that can be made before asking it for a
    // location is one reservation it does not have to reclaim later.
    String suppliedLocation = properties.get(Table.PROPERTY_LOCATION);
    boolean provisioned = StringUtils.isBlank(suppliedLocation);
    String tableLocation =
        provisioned
            ? provisionTableLocation(ident, properties, schema)
            : DefaultTableLocationProvider.ensureTrailingSlash(suppliedLocation);

    Map<String, String> newProperties = Maps.newHashMap(properties);
    newProperties.put(Table.PROPERTY_LOCATION, tableLocation);
    newProperties.put(Table.PROPERTY_TABLE_FORMAT, format);

    Table createdTable =
        tableOps.createTable(
            ident, columns, comment, newProperties, partitions, distribution, sortOrders, indexes);
    if (provisioned) {
      releaseLocationIfUnused(ident, schema, newProperties, tableLocation, createdTable);
    }
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
            .build();

    // The properties just read are handed on rather than left to be read again: resolving the
    // table format is a second store read for the very same entity whenever the format cache is
    // cold, which for a drop it usually is.
    ManagedTableOperations tableOps = tableOps(ident, tableProperties);
    boolean dropped = purge ? tableOps.purgeTable(ident) : tableOps.dropTable(ident);
    tableFormatCache.invalidate(ident);

    if (dropped && !context.isExternal()) {
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
          "Table {} is external, so its location '{}' is left alone rather than handed back to "
              + "table location provider '{}': the catalog does not own that data.",
          ident,
          context.tableProperties().get(Table.PROPERTY_LOCATION),
          tableLocationProvider.name());
    }

    return dropped;
  }

  private ManagedTableOperations tableOps(NameIdentifier tableIdent) {
    return tableOps(tableIdent, null);
  }

  /**
   * Returns the table operations for the format of the given table.
   *
   * @param tableIdent the identifier of the table
   * @param knownProperties the table's properties if the caller has already read them, null to have
   *     them read from the store when the format cache misses. Passing them in only saves a store
   *     read; it does not let a caller override the format of a table that is already cached.
   * @return the table operations for the table's format
   */
  private ManagedTableOperations tableOps(
      NameIdentifier tableIdent, @Nullable Map<String, String> knownProperties) {
    try {
      String tableFormat =
          tableFormatCache.get(
              tableIdent,
              () -> {
                Map<String, String> properties =
                    knownProperties != null
                        ? knownProperties
                        : store.get(tableIdent, TABLE, TableEntity.class).properties();
                String format = properties.getOrDefault(Table.PROPERTY_TABLE_FORMAT, null);
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
   * Asks the {@link TableLocationProvider} where the table being created should live.
   *
   * <p>Called only when the request does not carry a location of its own. A caller that supplies a
   * location has already decided where the data goes, and in this catalog it usually does so
   * because the data is already there: an external Delta table, or a Lance registration, both of
   * which point at a dataset that exists. Handing such a request to a provider that allocates
   * storage would do two wrong things at once -- repoint the table at a freshly allocated empty
   * path, leaving the caller's data orphaned while the creation still reports success, and leave
   * behind an allocation that is never written to and never handed back.
   *
   * <p>The catalog cannot tell those requests apart from a caller merely overriding placement, and
   * a rule each provider has to re-derive for itself is a rule most of them will get wrong. So the
   * provider is consulted only when the catalog is the one choosing, which is also what this
   * catalog has always done with a supplied location. A deployment that wants allocation to be
   * mandatory has to reject a caller-supplied location before it reaches the catalog.
   *
   * @param ident the identifier of the table being created
   * @param properties the table properties the caller supplied
   * @param schema the schema the table is created in
   * @return the provisioned location, never blank
   */
  private String provisionTableLocation(
      NameIdentifier ident, Map<String, String> properties, Schema schema) {
    TableLocationProvider provider = tableLocationProvider;
    String location =
        provider.provisionTableLocation(
            TableLocationContext.builder()
                .withTableIdentifier(ident)
                .withTableProperties(properties)
                .withSchema(schema)
                .build());

    // Only blankness is checked. The shape of the path belongs to the provider: nothing downstream
    // appends to the location, and the value is stored verbatim so that a provider unprovisioning
    // it later sees exactly the string it returned.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(location),
        "Table location provider '%s' returned a null or blank location for table %s",
        provider.name(),
        ident);

    return location;
  }

  /**
   * Hands back a location that was provisioned for this creation and that the created table does
   * not point at.
   *
   * <p>A format may decline the location it was given and still report success. Lance's {@code
   * EXIST_OK} creation mode returns the table that already exists, at the location that table
   * already had, and a client retrying a create is the ordinary way to reach that path. Without
   * this, every such call would leak one allocation with nothing in the logs to show for it.
   *
   * <p>This is {@link TableLocationProvider#releaseUnusedLocation(TableLocationContext)} and not
   * {@link TableLocationProvider#unprovisionTableLocation(TableLocationContext)}, even though for
   * the built-in provider the two do the same nothing. The table here is alive and is about to be
   * returned to the caller; a provider that reclaims by table identity rather than by path would
   * read the drop callback literally and delete the registration of a table that exists. Releasing
   * defaults to doing nothing for exactly that reason, so a provider opts in to it knowingly.
   *
   * <p>Releasing is safe here in a way it is not on the creation failure path: nothing can have
   * been written to a location the created table does not point at. A failure to release is logged
   * and otherwise ignored, as it is on drop -- the table was created, and reporting the creation as
   * failed would be the worse answer.
   *
   * <p>The two locations are compared with a trailing slash added to both, since that is the one
   * rewrite this catalog performs itself. A format that rewrites the location further -- collapsing
   * a duplicated separator, or normalizing a URI scheme -- is indistinguishable from here from a
   * format that declined the location outright, which the contract warns providers about.
   *
   * @param ident the identifier of the table that was created
   * @param schema the schema the table was created in
   * @param properties the properties the format was given, carrying the provisioned location
   * @param provisionedLocation the location the provider handed out
   * @param createdTable the table the format returned, which may be null
   */
  private void releaseLocationIfUnused(
      NameIdentifier ident,
      Schema schema,
      Map<String, String> properties,
      String provisionedLocation,
      Table createdTable) {
    // Neither built-in delegator returns null, but a third-party one that did would otherwise
    // fail a creation that has already succeeded, and only on this path: the same delegator would
    // serve a request that carried its own location without complaint. An asymmetry like that is
    // far harder to diagnose than the leaked location that returning here may cost.
    if (createdTable == null) {
      return;
    }

    Map<String, String> createdProperties = createdTable.properties();
    String storedLocation =
        createdProperties == null ? null : createdProperties.get(Table.PROPERTY_LOCATION);

    // Only a location that is visibly different is released. A format reporting no location at all
    // may still be using the one it was handed, and leaking it is the safer reading of that.
    if (StringUtils.isBlank(storedLocation)
        || DefaultTableLocationProvider.ensureTrailingSlash(provisionedLocation)
            .equals(DefaultTableLocationProvider.ensureTrailingSlash(storedLocation))) {
      return;
    }

    TableLocationProvider provider = tableLocationProvider;
    // The exact class, not instanceof: the built-in provider composes its location from
    // configuration and registers it nowhere, so nothing was allocated and nothing can leak. A
    // subclass of it may well allocate, and silencing that one would be the same lie in the other
    // direction. Warning unconditionally would be the more common lie, since a client retrying a
    // create reaches this path routinely on a default deployment.
    if (provider.getClass() == DefaultTableLocationProvider.class) {
      LOG.debug(
          "Table {} was created at '{}' rather than at the composed location '{}'. The composed "
              + "location was never allocated anywhere, so there is nothing to release.",
          ident,
          storedLocation,
          provisionedLocation);
    } else {
      LOG.warn(
          "Table {} was created at '{}' rather than at the provisioned location '{}'. Asking "
              + "table location provider '{}' to release the unused location; a provider that "
              + "does not implement the release callback leaves it allocated.",
          ident,
          storedLocation,
          provisionedLocation,
          provider.name());
    }

    try {
      provider.releaseUnusedLocation(
          TableLocationContext.builder()
              .withTableIdentifier(ident)
              .withTableProperties(properties)
              .withSchema(schema)
              .build());
    } catch (Exception e) {
      LOG.warn(
          "Table location provider '{}' failed to release the unused location '{}' provisioned for "
              + "table {}. The storage may be leaked and needs to be reclaimed manually.",
          provider.name(),
          provisionedLocation,
          ident,
          e);
    }
  }
}
