/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.EntityType.SCHEMA;
import static com.datastrato.gravitino.Entity.EntityType.TABLE;
import static com.datastrato.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static com.datastrato.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.datastrato.gravitino.utils.ThrowableFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A catalog operation dispatcher that dispatches the catalog operations to the underlying catalog
 * implementation.
 */
public class CatalogOperationDispatcher implements TableCatalog, FilesetCatalog, SupportsSchemas {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogOperationDispatcher.class);

  private final CatalogManager catalogManager;

  private final EntityStore store;

  private final IdGenerator idGenerator;

  /**
   * Creates a new CatalogOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for catalog operations.
   * @param store The EntityStore instance to be used for catalog operations.
   * @param idGenerator The IdGenerator instance to be used for catalog operations.
   */
  public CatalogOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    this.catalogManager = catalogManager;
    this.store = store;
    this.idGenerator = idGenerator;
  }

  /**
   * Lists the schemas within the specified namespace.
   *
   * @param namespace The namespace in which to list schemas.
   * @return An array of NameIdentifier objects representing the schemas within the specified
   *     namespace.
   * @throws NoSuchCatalogException If the catalog namespace does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return doWithCatalog(
        getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
        c -> c.doWithSchemaOps(s -> s.listSchemas(namespace)),
        NoSuchCatalogException.class);
  }

  /**
   * Creates a new schema.
   *
   * @param ident The identifier for the schema to be created.
   * @param comment The comment for the new schema.
   * @param properties Additional properties for the new schema.
   * @return The created Schema object.
   * @throws NoSuchCatalogException If the catalog corresponding to the provided identifier does not
   *     exist.
   * @throws SchemaAlreadyExistsException If a schema with the same identifier already exists.
   */
  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.schemaPropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    long uid = idGenerator.nextId();
    // Add StringIdentifier to the properties, the specific catalog will handle this
    // StringIdentifier to make sure only when the operation is successful, the related
    // SchemaEntity will be visible.
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    Schema createdSchema =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithSchemaOps(s -> s.createSchema(ident, comment, updatedProperties)),
            NoSuchCatalogException.class,
            SchemaAlreadyExistsException.class);

    // If the Schema is maintained by the Gravitino's store, we don't have to store again.
    boolean isManagedSchema = isManagedEntity(createdSchema.properties());
    if (isManagedSchema) {
      return EntityCombinedSchema.of(createdSchema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  createdSchema.properties()));
    }

    // Retrieve the Schema again to obtain some values generated by underlying catalog
    Schema schema =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
            NoSuchSchemaException.class);

    SchemaEntity schemaEntity =
        new SchemaEntity.Builder()
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
      store.put(schemaEntity, true /* overwrite */);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      return EntityCombinedSchema.of(schema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  schema.properties()));
    }

    // Merge both the metadata from catalog operation and the metadata from entity store.
    return EntityCombinedSchema.of(schema, schemaEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::schemaPropertiesMetadata, schema.properties()));
  }

  /**
   * Loads and retrieves a schema.
   *
   * @param ident The identifier of the schema to be loaded.
   * @return The loaded Schema object.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    Schema schema =
        doWithCatalog(
            catalogIdentifier,
            c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
            NoSuchSchemaException.class);

    // If the Schema is maintained by the Gravitino's store, we don't have to load again.
    boolean isManagedSchema = isManagedEntity(schema.properties());
    if (isManagedSchema) {
      return EntityCombinedSchema.of(schema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  schema.properties()));
    }

    StringIdentifier stringId = getStringIdFromProperties(schema.properties());
    // Case 1: The schema is not created by Gravitino.
    if (stringId == null) {
      return EntityCombinedSchema.of(schema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  schema.properties()));
    }

    SchemaEntity schemaEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, SCHEMA, SchemaEntity.class),
            "GET",
            stringId.id());
    return EntityCombinedSchema.of(schema, schemaEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdentifier,
                HasPropertyMetadata::schemaPropertiesMetadata,
                schema.properties()));
  }

  /**
   * Alters the schema by applying the provided schema changes.
   *
   * @param ident The identifier of the schema to be altered.
   * @param changes The array of SchemaChange objects representing the alterations to apply.
   * @return The altered Schema object.
   * @throws NoSuchSchemaException If the schema corresponding to the provided identifier does not
   *     exist.
   */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    validateAlterProperties(ident, HasPropertyMetadata::schemaPropertiesMetadata, changes);

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Schema tempAlteredSchema =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithSchemaOps(s -> s.alterSchema(ident, changes)),
            NoSuchSchemaException.class);

    // Retrieve the Schema again to obtain some values generated by underlying catalog
    Schema alteredSchema =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithSchemaOps(
                    s ->
                        s.loadSchema(
                            NameIdentifier.of(ident.namespace(), tempAlteredSchema.name()))),
            NoSuchSchemaException.class);

    // If the Schema is maintained by the Gravitino's store, we don't have to alter again.
    boolean isManagedSchema = isManagedEntity(alteredSchema.properties());
    if (isManagedSchema) {
      return EntityCombinedSchema.of(alteredSchema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  alteredSchema.properties()));
    }

    StringIdentifier stringId = getStringIdFromProperties(alteredSchema.properties());
    // Case 1: The schema is not created by Gravitino.
    if (stringId == null) {
      return EntityCombinedSchema.of(alteredSchema)
          .withHiddenPropertiesSet(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  alteredSchema.properties()));
    }

    SchemaEntity updatedSchemaEntity =
        operateOnEntity(
            ident,
            id ->
                store.update(
                    id,
                    SchemaEntity.class,
                    SCHEMA,
                    schemaEntity ->
                        new SchemaEntity.Builder()
                            .withId(schemaEntity.id())
                            .withName(schemaEntity.name())
                            .withNamespace(ident.namespace())
                            .withAuditInfo(
                                AuditInfo.builder()
                                    .withCreator(schemaEntity.auditInfo().creator())
                                    .withCreateTime(schemaEntity.auditInfo().createTime())
                                    .withLastModifier(
                                        PrincipalUtils.getCurrentPrincipal().getName())
                                    .withLastModifiedTime(Instant.now())
                                    .build())
                            .build()),
            "UPDATE",
            stringId.id());
    return EntityCombinedSchema.of(alteredSchema, updatedSchemaEntity)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::schemaPropertiesMetadata,
                alteredSchema.properties()));
  }

  /**
   * Drops a schema.
   *
   * @param ident The identifier of the schema to be dropped.
   * @param cascade If true, drops all tables within the schema as well.
   * @return True if the schema was successfully dropped, false otherwise.
   * @throws NonEmptySchemaException If the schema contains tables and cascade is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    boolean dropped =
        doWithCatalog(
            getCatalogIdentifier(ident),
            c -> c.doWithSchemaOps(s -> s.dropSchema(ident, cascade)),
            NonEmptySchemaException.class);

    if (!dropped) {
      return false;
    }

    try {
      return store.delete(ident, SCHEMA, cascade);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
        new TableEntity.Builder()
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
            c -> c.doWithTableOps(t -> t.alterTable(ident, changes)),
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

                      return new TableEntity.Builder()
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

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return doWithCatalog(
        getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
        c -> c.doWithFilesetOps(f -> f.listFilesets(namespace)),
        NoSuchSchemaException.class);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Fileset fileset =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithFilesetOps(f -> f.loadFileset(ident)),
            NoSuchFilesetException.class);

    // Currently we only support maintaining the Fileset in the Gravitino's store.
    return EntityCombinedFileset.of(fileset)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                fileset.properties()));
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.filesetPropertiesMetadata(), properties);
                  return null;
                }),
        IllegalArgumentException.class);
    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    Fileset createdFileset =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithFilesetOps(
                    f -> f.createFileset(ident, comment, type, storageLocation, updatedProperties)),
            NoSuchSchemaException.class,
            FilesetAlreadyExistsException.class);
    return EntityCombinedFileset.of(createdFileset)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                createdFileset.properties()));
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::filesetPropertiesMetadata, changes);

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Fileset alteredFileset =
        doWithCatalog(
            catalogIdent,
            c -> c.doWithFilesetOps(f -> f.alterFileset(ident, changes)),
            NoSuchFilesetException.class,
            IllegalArgumentException.class);
    return EntityCombinedFileset.of(alteredFileset)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::filesetPropertiesMetadata,
                alteredFileset.properties()));
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    return doWithCatalog(
        getCatalogIdentifier(ident),
        c -> c.doWithFilesetOps(f -> f.dropFileset(ident)),
        NonEmptyEntityException.class);
  }

  private <R, E extends Throwable> R doWithCatalog(
      NameIdentifier ident, ThrowableFunction<CatalogManager.CatalogWrapper, R> fn, Class<E> ex)
      throws E {
    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex.isInstance(throwable)) {
        throw ex.cast(throwable);
      }
      throw new RuntimeException(throwable);
    }
  }

  private <R, E1 extends Throwable, E2 extends Throwable> R doWithCatalog(
      NameIdentifier ident,
      ThrowableFunction<CatalogManager.CatalogWrapper, R> fn,
      Class<E1> ex1,
      Class<E2> ex2)
      throws E1, E2 {
    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex1.isInstance(throwable)) {
        throw ex1.cast(throwable);
      } else if (ex2.isInstance(throwable)) {
        throw ex2.cast(throwable);
      }
      if (RuntimeException.class.isAssignableFrom(throwable.getClass())) {
        throw (RuntimeException) throwable;
      }

      throw new RuntimeException(throwable);
    }
  }

  private Set<String> getHiddenPropertyNames(
      NameIdentifier catalogIdent,
      ThrowableFunction<HasPropertyMetadata, PropertiesMetadata> provider,
      Map<String, String> properties) {
    return doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  PropertiesMetadata propertiesMetadata = provider.apply(p);
                  return properties.keySet().stream()
                      .filter(propertiesMetadata::isHiddenProperty)
                      .collect(Collectors.toSet());
                }),
        IllegalArgumentException.class);
  }

  private <T> void validateAlterProperties(
      NameIdentifier ident,
      ThrowableFunction<HasPropertyMetadata, PropertiesMetadata> provider,
      T... changes) {
    doWithCatalog(
        getCatalogIdentifier(ident),
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  Map<String, String> upserts = getPropertiesForSet(changes);
                  Map<String, String> deletes = getPropertiesForDelete(changes);
                  validatePropertyForAlter(provider.apply(p), upserts, deletes);
                  return null;
                }),
        IllegalArgumentException.class);
  }

  private <T> Map<String, String> getPropertiesForSet(T... t) {
    Map<String, String> properties = Maps.newHashMap();
    for (T item : t) {
      if (item instanceof TableChange.SetProperty) {
        TableChange.SetProperty setProperty = (TableChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (item instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (item instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      }
    }

    return properties;
  }

  private <T> Map<String, String> getPropertiesForDelete(T... t) {
    Map<String, String> properties = Maps.newHashMap();
    for (T item : t) {
      if (item instanceof TableChange.RemoveProperty) {
        TableChange.RemoveProperty removeProperty = (TableChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      } else if (item instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty removeProperty = (SchemaChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      } else if (item instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      }
    }

    return properties;
  }

  private StringIdentifier getStringIdFromProperties(Map<String, String> properties) {
    try {
      StringIdentifier stringId = StringIdentifier.fromProperties(properties);
      if (stringId == null) {
        LOG.warn(FormattedErrorMessages.STRING_ID_NOT_FOUND);
      }
      return stringId;
    } catch (IllegalArgumentException e) {
      LOG.warn(FormattedErrorMessages.STRING_ID_PARSE_ERROR, e.getMessage());
      return null;
    }
  }

  private <R extends HasIdentifier> R operateOnEntity(
      NameIdentifier ident, ThrowableFunction<NameIdentifier, R> fn, String opName, long id) {
    R ret = null;
    try {
      ret = fn.apply(ident);
    } catch (NoSuchEntityException e) {
      // Case 2: The table is created by Gravitino, but has no corresponding entity in Gravitino.
      LOG.error(FormattedErrorMessages.ENTITY_NOT_FOUND, ident);
    } catch (Exception e) {
      // Case 3: The table is created by Gravitino, but failed to operate the corresponding entity
      // in Gravitino
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, opName, ident, e);
    }

    // Case 4: The table is created by Gravitino, but the uid in the corresponding entity is not
    // matched.
    if (ret != null && ret.id() != id) {
      LOG.error(FormattedErrorMessages.ENTITY_UNMATCHED, ident, ret.id(), id);
      ret = null;
    }

    return ret;
  }

  @VisibleForTesting
  // TODO(xun): Remove this method when we implement a better way to get the catalog identifier
  //  [#257] Add an explicit get catalog functions in NameIdentifier
  NameIdentifier getCatalogIdentifier(NameIdentifier ident) {
    NameIdentifier.check(
        ident.name() != null, "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && ident.namespace().length() > 0,
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 2) {
      throw new IllegalNameIdentifierException(
          "Cannot create a catalog NameIdentifier less than two elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1));
  }

  private boolean isManagedEntity(Map<String, String> properties) {
    return Optional.ofNullable(properties)
        .map(
            p ->
                p.getOrDefault(
                        BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY, Boolean.FALSE.toString())
                    .equals(Boolean.TRUE.toString()))
        .orElse(false);
  }

  private static final class FormattedErrorMessages {
    static final String STORE_OP_FAILURE =
        "Failed to {} entity for {} in "
            + "Gravitino, with this situation the returned object will not contain the metadata from "
            + "Gravitino.";

    static final String STRING_ID_NOT_FOUND =
        "String identifier is not set in schema properties, "
            + "this is because the schema is not created by Gravitino, or the schema is created by "
            + "Gravitino but the string identifier is removed by the user.";

    static final String STRING_ID_PARSE_ERROR =
        "Failed to get string identifier from schema "
            + "properties: {}, this maybe caused by the same-name string identifier is set by the user "
            + "with unsupported format.";

    static final String ENTITY_NOT_FOUND =
        "Entity for {} doesn't exist in Gravitino, "
            + "this is unexpected if this is created by Gravitino. With this situation the "
            + "returned object will not contain the metadata from Gravitino";

    static final String ENTITY_UNMATCHED =
        "Entity {} with uid {} doesn't match the string "
            + "identifier in the property {}, this is unexpected if this object is created by "
            + "Gravitino. This might be due to some operations that are not performed through Gravitino. "
            + "With this situation the returned object will not contain the metadata from Gravitino";
  }
}
