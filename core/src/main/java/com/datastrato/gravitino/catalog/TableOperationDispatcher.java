/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.EntityType.TABLE;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableOperationDispatcher extends OperationDispatcher implements TableDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(TableOperationDispatcher.class);

  /**
   * Creates a new TableOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for table operations.
   * @param store The EntityStore instance to be used for table operations.
   * @param idGenerator The IdGenerator instance to be used for table operations.
   */
  public TableOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * Lists the tables within a schema.
   *
   * @param namespace The namespace of the schema containing the tables.
   * @return An array of {@link NameIdentifier} objects representing the identifiers of the tables
   *     in the schema.
   * @throws NoSuchSchemaException If the specified schema does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return doWithCatalog(
        getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
        c -> c.doWithTableOps(t -> t.listTables(namespace)),
        NoSuchSchemaException.class);
  }

  /**
   * Loads a table.
   *
   * @param ident The identifier of the table to load.
   * @return The loaded {@link Table} object representing the requested table.
   * @throws NoSuchTableException If the specified table does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    Table table =
        doWithCatalog(
            catalogIdentifier,
            c -> c.doWithTableOps(t -> t.loadTable(ident)),
            NoSuchTableException.class);

    StringIdentifier stringId = getStringIdFromProperties(table.properties());
    // Case 1: The table is not created by Gravitino.
    if (stringId == null) {
      return EntityCombinedTable.of(table)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  table.properties()));
    }

    TableEntity tableEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, TABLE, TableEntity.class),
            "GET",
            stringId.id());

    return EntityCombinedTable.of(table, tableEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdentifier,
                HasPropertyMetadata::tablePropertiesMetadata,
                table.properties()));
  }

  /**
   * Creates a new table in a schema.
   *
   * @param ident The identifier of the table to create.
   * @param columns An array of {@link Column} objects representing the columns of the table.
   * @param comment A description or comment associated with the table.
   * @param properties Additional properties to set for the table.
   * @param partitions An array of {@link Transform} objects representing the partitioning of table
   * @param indexes An array of {@link Index} objects representing the indexes of the table.
   * @return The newly created {@link Table} object.
   * @throws NoSuchSchemaException If the schema in which to create the table does not exist.
   * @throws TableAlreadyExistsException If a table with the same name already exists in the schema.
   */
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
    if (Entity.SECURABLE_ENTITY_RESERVED_NAME.equals(ident.name())) {
      throw new IllegalArgumentException("Can't create a table with with reserved name `*`");
    }

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.tablePropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    long uid = idGenerator.nextId();
    // Add StringIdentifier to the properties, the specific catalog will handle this
    // StringIdentifier to make sure only when the operation is successful, the related
    // TableEntity will be visible.
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithTableOps(
                t ->
                    t.createTable(
                        ident,
                        columns,
                        comment,
                        updatedProperties,
                        partitions == null ? EMPTY_TRANSFORM : partitions,
                        distribution == null ? Distributions.NONE : distribution,
                        sortOrders == null ? new SortOrder[0] : sortOrders,
                        indexes == null ? Indexes.EMPTY_INDEXES : indexes)),
        NoSuchSchemaException.class,
        TableAlreadyExistsException.class);

    // Retrieve the Table again to obtain some values generated by underlying catalog
    Table table =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithTableOps(t -> t.loadTable(ident)),
            NoSuchTableException.class);

    TableEntity tableEntity =
        TableEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(tableEntity, true /* overwrite */);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      return EntityCombinedTable.of(table)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent, HasPropertyMetadata::tablePropertiesMetadata, table.properties()));
    }

    return EntityCombinedTable.of(table, tableEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::tablePropertiesMetadata, table.properties()));
  }

  /**
   * Alters an existing table.
   *
   * @param ident The identifier of the table to alter.
   * @param changes An array of {@link TableChange} objects representing the changes to apply to the
   *     table.
   * @return The altered {@link Table} object after applying the changes.
   * @throws NoSuchTableException If the table to alter does not exist.
   * @throws IllegalArgumentException If an unsupported or invalid change is specified.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::tablePropertiesMetadata, changes);

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Table tempAlteredTable =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithTableOps(
                    t -> t.alterTable(ident, applyCapabilities(c.capabilities(), changes))),
            NoSuchTableException.class,
            IllegalArgumentException.class);

    // Retrieve the Table again to obtain some values generated by underlying catalog
    Table alteredTable =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithTableOps(
                    t ->
                        t.loadTable(NameIdentifier.of(ident.namespace(), tempAlteredTable.name()))),
            NoSuchTableException.class);

    StringIdentifier stringId = getStringIdFromProperties(alteredTable.properties());
    // Case 1: The table is not created by Gravitino.
    if (stringId == null) {
      return EntityCombinedTable.of(alteredTable)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  getCatalogIdentifier(ident),
                  HasPropertyMetadata::tablePropertiesMetadata,
                  alteredTable.properties()));
    }

    TableEntity updatedTableEntity =
        operateOnEntity(
            ident,
            id ->
                store.update(
                    id,
                    TableEntity.class,
                    TABLE,
                    tableEntity -> {
                      String newName =
                          Arrays.stream(changes)
                              .filter(c -> c instanceof TableChange.RenameTable)
                              .map(c -> ((TableChange.RenameTable) c).getNewName())
                              .reduce((c1, c2) -> c2)
                              .orElse(tableEntity.name());

                      return TableEntity.builder()
                          .withId(tableEntity.id())
                          .withName(newName)
                          .withNamespace(ident.namespace())
                          .withAuditInfo(
                              AuditInfo.builder()
                                  .withCreator(tableEntity.auditInfo().creator())
                                  .withCreateTime(tableEntity.auditInfo().createTime())
                                  .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                                  .withLastModifiedTime(Instant.now())
                                  .build())
                          .build();
                    }),
            "UPDATE",
            stringId.id());

    return EntityCombinedTable.of(alteredTable, updatedTableEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                getCatalogIdentifier(ident),
                HasPropertyMetadata::tablePropertiesMetadata,
                alteredTable.properties()));
  }

  /**
   * Drops a table from the catalog.
   *
   * @param ident The identifier of the table to drop.
   * @return {@code true} if the table was successfully dropped, {@code false} if the table does not
   *     exist.
   * @throws NoSuchTableException If the table to drop does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    boolean dropped =
        doWithCatalog(
            getCatalogIdentifier(ident),
            c -> c.doWithTableOps(t -> t.dropTable(ident)),
            NoSuchTableException.class);

    if (!dropped) {
      return false;
    }

    try {
      store.delete(ident, TABLE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  /**
   * Drop a table from the catalog and completely remove its data. Removes both the metadata and the
   * directory associated with the table completely and skipping trash. If the table is an external
   * table or the catalogs don't support purge table, {@link UnsupportedOperationException} is
   * thrown.
   *
   * <p>If the catalog supports to purge a table, this method should be overridden. The default
   * implementation throws an {@link UnsupportedOperationException}.
   *
   * @param ident A table identifier.
   * @return True if the table was purged, false if the table did not exist.
   * @throws UnsupportedOperationException If the catalog does not support to purge a table.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    boolean purged =
        doWithCatalog(
            getCatalogIdentifier(ident),
            c -> c.doWithTableOps(t -> t.purgeTable(ident)),
            NoSuchTableException.class,
            UnsupportedOperationException.class);

    if (!purged) {
      return false;
    }

    try {
      store.delete(ident, TABLE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return true;
  }
}
