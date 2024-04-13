/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.EntityType.SCHEMA;
import static com.datastrato.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaOperationDispatcher extends OperationDispatcher implements SupportsSchemas {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaOperationDispatcher.class);

  /**
   * Creates a new SchemaOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for schema operations.
   * @param store The EntityStore instance to be used for schema operations.
   * @param idGenerator The IdGenerator instance to be used for schema operations.
   */
  public SchemaOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
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
    if (Entity.RESOURCE_ENTITY_RESERVED_NAME.equals(ident.name())) {
      throw new IllegalArgumentException("Can't create a schema with with reserved name `*`");
    }

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
        SchemaEntity.builder()
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
                        SchemaEntity.builder()
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
}
