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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Entity.EntityType.FILESET;
import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;
import static org.apache.gravitino.utils.NameIdentifierUtil.ofFileset;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.secret.SecretAlterChanges;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretMaterial;
import org.apache.gravitino.secret.SecretMaterialsHolder;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.secret.SecretReference;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.SchemaEntityCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaOperationDispatcher extends OperationDispatcher implements SchemaDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaOperationDispatcher.class);

  @Nullable private final FilesetDispatcher filesetDispatcher;

  /**
   * Creates a new SchemaOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for schema operations.
   * @param store The EntityStore instance to be used for schema operations.
   * @param idGenerator The IdGenerator instance to be used for schema operations.
   * @param secretManager The SecretManager instance to be used for secret operations.
   */
  public SchemaOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    this(catalogManager, store, idGenerator, secretManager, null);
  }

  /**
   * Creates a new SchemaOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for schema operations.
   * @param store The EntityStore instance to be used for schema operations.
   * @param idGenerator The IdGenerator instance to be used for schema operations.
   * @param secretManager The SecretManager instance to be used for secret operations.
   * @param filesetDispatcher The fileset dispatcher used to drop filesets on cascade schema drop.
   */
  public SchemaOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager,
      FilesetDispatcher filesetDispatcher) {
    super(catalogManager, store, idGenerator, secretManager);
    this.filesetDispatcher = filesetDispatcher;
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
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(namespace.levels()),
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
                c -> c.doWithSchemaOps(s -> s.listSchemas(namespace)),
                NoSuchCatalogException.class));
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
    return createSchema(ident, comment, properties, Collections.emptyMap(), Collections.emptyMap());
  }

  @Override
  public Schema createSchema(
      NameIdentifier ident,
      String comment,
      Map<String, String> properties,
      Map<String, SecretBinding> secretBindings,
      Map<String, SecretReference> secretReferences)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    long uid = idGenerator.nextId();
    Map<String, String> entityProperties =
        SecretPropertyUtils.copyEntityProperties(properties, secretBindings, secretReferences);
    List<SecretMaterial> secretMaterials =
        secretManager.assembleSecretMaterials(
            properties, entityProperties, "schema", uid, secretBindings, secretReferences);
    doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  validatePropertyForCreate(p.schemaPropertiesMetadata(), entityProperties);
                  return null;
                }),
        IllegalArgumentException.class);
    // Add StringIdentifier to the properties, the specific catalog will handle this
    // StringIdentifier to make sure only when the operation is successful, the related
    // SchemaEntity will be visible.
    //
    // Same split as CatalogManager: create/storage properties keep secret URNs. Connectors that
    // need plaintext for runtime (e.g. Fileset FS) resolve at the conf boundary — see
    // FilesetCatalogOperations.mergeUpLevelConfigurations / CatalogManager.createBaseCatalog.
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, entityProperties);

    // Write secrets before createSchema: create paths that resolve URNs (e.g. Fileset FS
    // mergeUpLevelConfigurations, catalog createBaseCatalog) require secrets to exist first.
    // Roll back on any create failure (same pattern as CatalogManager /
    // FilesetOperationDispatcher).
    secretManager.writeSecrets(secretMaterials);
    try {
      return TreeLockUtils.doWithTreeLock(
          catalogIdent,
          LockType.WRITE,
          () -> {
            // we do not retrieve the schema again (to obtain some values generated by underlying
            // catalog)
            // since some catalogs' API is async and the schema may not be created immediately
            Schema schema =
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithSchemaOps(s -> s.createSchema(ident, comment, updatedProperties)),
                    NoSuchCatalogException.class,
                    SchemaAlreadyExistsException.class);

            // If the Schema is maintained by the Gravitino's store, we don't have to store again.
            boolean isManagedSchema = isManagedEntity(catalogIdent, Capability.Scope.SCHEMA);
            if (isManagedSchema) {
              return EntityCombinedSchema.of(schema)
                  .withHiddenProperties(
                      getHiddenPropertyNames(
                          catalogIdent,
                          HasPropertyMetadata::schemaPropertiesMetadata,
                          schema.properties()));
            }

            // Persist properties (including secret URNs) in the entity store so cleanup still works
            // when the underlying catalog does not retain schema properties.
            SchemaEntity schemaEntity =
                SchemaEntity.builder()
                    .withId(uid)
                    .withName(ident.name())
                    .withNamespace(ident.namespace())
                    .withProperties(updatedProperties)
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
                  .withHiddenProperties(
                      getHiddenPropertyNames(
                          catalogIdent,
                          HasPropertyMetadata::schemaPropertiesMetadata,
                          schema.properties()));
            }

            // Merge both the metadata from catalog operation and the metadata from entity store.
            return EntityCombinedSchema.of(schema, schemaEntity)
                .withHiddenProperties(
                    getHiddenPropertyNames(
                        catalogIdent,
                        HasPropertyMetadata::schemaPropertiesMetadata,
                        schema.properties()));
          });
    } catch (RuntimeException e) {
      secretManager.rollbackSecrets(secretMaterials);
      throw e;
    }
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
    // Load the schema and check if this schema is already imported.
    EntityCombinedSchema schema =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadSchema(ident));

    if (!schema.imported()) {
      try {
        TreeLockUtils.doWithTreeLock(
            NameIdentifier.of(ident.namespace().levels()),
            LockType.WRITE,
            () -> {
              importSchema(ident);
              return null;
            });
      } catch (EntityAlreadyExistsException e) {
        // HA race: another Gravitino node concurrently imported this schema. Reload from the
        // entity store to verify the entity stored by the winning node is consistent.
        LOG.info(
            "Schema {} was concurrently imported by another node; reloading from store.", ident);
        EntityCombinedSchema reloaded =
            TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadSchema(ident));
        if (!reloaded.imported()) {
          throw new UnsupportedOperationException(
              "Schema managed by multiple catalogs. This may cause unexpected issues such as privilege conflicts. "
                  + "To resolve: Remove all catalogs managing this schema, then recreate one catalog to ensure single-catalog management.");
        }
      }
    }

    return schema;
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

    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    // Gravitino does not support alter schema currently, so we do not need to check whether there
    // exists SchemaChange.renameSchema in the changes and can lock schema directly.
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () -> {
          Pair<Schema, SchemaChange[]> alterResult =
              alterSchemaUnderLock(ident, catalogIdent, changes);
          Schema alteredSchema = alterResult.getLeft();
          SchemaChange[] effectiveChanges = alterResult.getRight();

          // If the Schema is maintained by the Gravitino's store, we don't have to alter again.
          boolean isManagedSchema = isManagedEntity(catalogIdent, Capability.Scope.SCHEMA);
          if (isManagedSchema) {
            return EntityCombinedSchema.of(alteredSchema)
                .withHiddenProperties(
                    getHiddenPropertyNames(
                        catalogIdent,
                        HasPropertyMetadata::schemaPropertiesMetadata,
                        alteredSchema.properties()));
          }

          StringIdentifier stringId = getStringIdFromProperties(alteredSchema.properties());
          // Case 1: The schema is not created by Gravitino and this schema is never imported.
          SchemaEntity se = null;
          if (stringId == null) {
            se = getEntity(ident, SCHEMA, SchemaEntity.class);
            if (se == null) {
              return EntityCombinedSchema.of(alteredSchema)
                  .withHiddenProperties(
                      getHiddenPropertyNames(
                          catalogIdent,
                          HasPropertyMetadata::schemaPropertiesMetadata,
                          alteredSchema.properties()));
            }
          }

          long schemaId;
          if (stringId != null) {
            schemaId = stringId.id();
          } else {
            schemaId = se.id();
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
                                  .withProperties(
                                      propertiesForSchemaEntityAlter(
                                          schemaEntity, effectiveChanges))
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
                  schemaId);

          return EntityCombinedSchema.of(alteredSchema, updatedSchemaEntity)
              .withHiddenProperties(
                  getHiddenPropertyNames(
                      catalogIdent,
                      HasPropertyMetadata::schemaPropertiesMetadata,
                      alteredSchema.properties()));
        });
  }

  private Pair<Schema, SchemaChange[]> alterSchemaUnderLock(
      NameIdentifier ident, NameIdentifier catalogIdent, SchemaChange... changes)
      throws NoSuchSchemaException {
    if (isManagedEntity(catalogIdent, Capability.Scope.SCHEMA)
        && usesManagedSchemaOperations(catalogIdent)) {
      return alterManagedSchemaUnderLock(ident, changes);
    }
    return alterExternalSchemaUnderLock(ident, catalogIdent, changes);
  }

  private boolean usesManagedSchemaOperations(NameIdentifier catalogIdent) {
    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s instanceof ManagedSchemaOperations),
        NoSuchSchemaException.class);
  }

  private Pair<Schema, SchemaChange[]> alterManagedSchemaUnderLock(
      NameIdentifier ident, SchemaChange... changes) throws NoSuchSchemaException {
    validateAlterProperties(ident, HasPropertyMetadata::schemaPropertiesMetadata, changes);

    SecretMaterialsHolder writtenSecretMaterials = new SecretMaterialsHolder();
    SchemaChange[][] effectiveChangesHolder = new SchemaChange[1][];
    boolean alterCommitted = false;
    try {
      SchemaEntity updatedEntity =
          store.update(
              ident,
              SchemaEntity.class,
              SCHEMA,
              existing -> {
                Map<String, String> currentProperties =
                    existing.properties() == null
                        ? new HashMap<>()
                        : new HashMap<>(existing.properties());
                Pair<SchemaChange[], List<SecretMaterial>> secretResult =
                    SecretAlterChanges.prepareSchemaChanges(
                        secretManager, currentProperties, existing.id(), changes);
                writtenSecretMaterials.set(secretResult.getRight());
                effectiveChangesHolder[0] = secretResult.getLeft();
                return SchemaEntityChanges.apply(ident, existing, secretResult.getLeft());
              });
      alterCommitted = true;
      Schema alteredSchema =
          ManagedSchemaOperations.ManagedSchema.builder()
              .withName(ident.name())
              .withComment(updatedEntity.comment())
              .withProperties(updatedEntity.properties())
              .withAuditInfo(updatedEntity.auditInfo())
              .build();
      return Pair.of(alteredSchema, effectiveChangesHolder[0]);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to alter schema " + ident, e);
    } finally {
      if (!alterCommitted) {
        secretManager.rollbackSecrets(writtenSecretMaterials.get());
      }
    }
  }

  private Pair<Schema, SchemaChange[]> alterExternalSchemaUnderLock(
      NameIdentifier ident, NameIdentifier catalogIdent, SchemaChange... changes)
      throws NoSuchSchemaException {
    Schema currentSchema = null;
    try {
      currentSchema =
          doWithCatalog(
              catalogIdent,
              c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
              NoSuchSchemaException.class);
    } catch (NoSuchSchemaException e) {
      // Defer missing-schema handling to catalog alterSchema to preserve catalog semantics.
    }
    // Prefer SchemaEntity properties for secret URNs (catalog loadSchema may omit them).
    SchemaEntity schemaEntityForSecrets = getEntity(ident, SCHEMA, SchemaEntity.class);
    Map<String, String> currentProperties;
    if (schemaEntityForSecrets != null
        && schemaEntityForSecrets.properties() != null
        && !schemaEntityForSecrets.properties().isEmpty()) {
      currentProperties = new HashMap<>(schemaEntityForSecrets.properties());
    } else if (currentSchema != null && currentSchema.properties() != null) {
      currentProperties = new HashMap<>(currentSchema.properties());
    } else {
      currentProperties = new HashMap<>();
    }

    validateAlterProperties(ident, HasPropertyMetadata::schemaPropertiesMetadata, changes);

    StringIdentifier currentStringId = getStringIdFromProperties(currentProperties);
    long entityIdForSecrets;
    if (currentStringId != null) {
      entityIdForSecrets = currentStringId.id();
    } else if (schemaEntityForSecrets != null) {
      entityIdForSecrets = schemaEntityForSecrets.id();
    } else {
      entityIdForSecrets = 0L;
    }

    SecretMaterialsHolder writtenSecretMaterials = new SecretMaterialsHolder();
    boolean alterCommitted = false;
    try {
      Pair<SchemaChange[], List<SecretMaterial>> secretResult =
          SecretAlterChanges.prepareSchemaChanges(
              secretManager, currentProperties, entityIdForSecrets, changes);
      writtenSecretMaterials.set(secretResult.getRight());
      SchemaChange[] effectiveChanges = secretResult.getLeft();

      Schema alteredSchema =
          doWithCatalog(
              catalogIdent,
              c -> c.doWithSchemaOps(s -> s.alterSchema(ident, effectiveChanges)),
              NoSuchSchemaException.class);
      alterCommitted = true;
      return Pair.of(alteredSchema, effectiveChanges);
    } finally {
      if (!alterCommitted) {
        secretManager.rollbackSecrets(writtenSecretMaterials.get());
      }
    }
  }

  /**
   * Drops a schema.
   *
   * @param ident The identifier of the schema to be dropped.
   * @param cascade If true, drops all tables within the schema as well.
   * @return True if the schema was successfully dropped, false if the schema doesn't exist.
   * @throws NonEmptySchemaException If the schema contains tables and cascade is set to false.
   * @throws RuntimeException If an error occurs while dropping the schema.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    // Cascade: drop filesets via FilesetDispatcher first so each fileset cleans its own
    // write-through secrets (including under non-fileset catalogs). Do this before the catalog
    // lock to avoid nested TreeLocks.
    if (cascade && filesetDispatcher != null) {
      Namespace filesetNs =
          Namespace.of(ident.namespace().level(0), ident.namespace().level(1), ident.name());
      List<FilesetEntity> filesets;
      try {
        filesets = store.list(filesetNs, FilesetEntity.class, FILESET);
      } catch (NoSuchEntityException e) {
        filesets = Collections.emptyList();
      } catch (IOException e) {
        throw new RuntimeException("Failed to list filesets under schema " + ident, e);
      }
      for (FilesetEntity fileset : filesets) {
        filesetDispatcher.dropFileset(
            ofFileset(filesetNs.level(0), filesetNs.level(1), filesetNs.level(2), fileset.name()));
      }
    }

    return TreeLockUtils.doWithTreeLock(
        catalogIdent,
        LockType.WRITE,
        () -> {
          // Schema secret URNs live on SchemaEntity in the store (catalog loadSchema may omit
          // them). Fileset write-through secrets on cascade are cleaned via FilesetDispatcher
          // above (and FilesetCatalogOperations.dropSchema as a fallback for fileset catalogs).
          Map<String, String> schemaProperties = new HashMap<>();
          SchemaEntity schemaEntity = getEntity(ident, SCHEMA, SchemaEntity.class);
          if (schemaEntity != null
              && schemaEntity.properties() != null
              && !schemaEntity.properties().isEmpty()) {
            schemaProperties = new HashMap<>(schemaEntity.properties());
          }

          boolean droppedFromCatalog =
              doWithCatalog(
                  catalogIdent,
                  c -> c.doWithSchemaOps(s -> s.dropSchema(ident, cascade)),
                  NonEmptySchemaException.class,
                  RuntimeException.class);

          // For managed schema, we don't need to drop the schema from the store again.
          boolean isManagedSchema = isManagedEntity(catalogIdent, Capability.Scope.SCHEMA);
          if (isManagedSchema) {
            if (droppedFromCatalog) {
              secretManager.deleteSecretsFromProperties(schemaProperties);
            }
            return droppedFromCatalog;
          }

          // A false result is ambiguous: the external schema may have been renamed or dropped out
          // of band. Preserve the registration because deleting it after a rename would lose
          // Gravitino-only metadata. A true out-of-band drop can therefore leave a stale
          // registration that requires separate cleanup.
          if (droppedFromCatalog) {
            try {
              store.delete(ident, SCHEMA, true);
            } catch (NoSuchEntityException e) {
              LOG.warn("The schema to be dropped does not exist in the store: {}", ident, e);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          SchemaEntityCleaner.deleteOrphanedSchemaEntities(
              store,
              ident,
              false,
              schemaIdent ->
                  doWithCatalog(
                      catalogIdent,
                      c -> c.doWithSchemaOps(s -> s.schemaExists(schemaIdent)),
                      RuntimeException.class));
          if (droppedFromCatalog) {
            secretManager.deleteSecretsFromProperties(schemaProperties);
          }
          return droppedFromCatalog;
        });
  }

  /**
   * Builds properties to persist on {@link SchemaEntity} after alter, matching catalog alter: start
   * from existing entity properties and apply set/remove changes so write-through secret URNs are
   * preserved when the underlying catalog omits them.
   */
  private static Map<String, String> propertiesForSchemaEntityAlter(
      SchemaEntity existing, SchemaChange[] changes) {
    Map<String, String> newProps =
        existing.properties() == null ? new HashMap<>() : new HashMap<>(existing.properties());
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        newProps.remove(((SchemaChange.RemoveProperty) change).getProperty());
      }
    }
    return newProps;
  }

  private void importSchema(NameIdentifier identifier) {
    EntityCombinedSchema schema = internalLoadSchema(identifier);
    if (schema.imported()) {
      return;
    }

    StringIdentifier stringId = null;
    try {
      stringId = schema.stringIdentifier();
    } catch (IllegalArgumentException ie) {
      LOG.warn(FormattedErrorMessages.STRING_ID_PARSE_ERROR, ie.getMessage());
    }

    long uid;
    if (stringId != null) {
      // If the entity in the store doesn't match the one in the external system, we use the data
      // of external system to correct it.
      LOG.warn(
          "The Schema uid {} existed but still needs to be imported, this could be happened "
              + "when Schema is renamed by external systems not controlled by Gravitino. In this case, "
              + "we need to overwrite the stored entity to keep consistency.",
          stringId);
      uid = stringId.id();
    } else {
      // If the entity doesn't exist, we import the entity from the external system.
      uid = idGenerator.nextId();
    }

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withId(uid)
            .withName(identifier.name())
            .withNamespace(identifier.namespace())
            .withProperties(
                schema.properties() == null ? Collections.emptyMap() : schema.properties())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(schema.auditInfo().creator())
                    .withCreateTime(schema.auditInfo().createTime())
                    .withLastModifier(schema.auditInfo().lastModifier())
                    .withLastModifiedTime(schema.auditInfo().lastModifiedTime())
                    .build())
            .build();
    try {
      store.put(schemaEntity, true);
    } catch (EntityAlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", identifier, e);
      throw new RuntimeException("Failed to import schema entity to the store", e);
    }
  }

  private EntityCombinedSchema internalLoadSchema(NameIdentifier ident) {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    Schema schema =
        doWithCatalog(
            catalogIdentifier,
            c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
            NoSuchSchemaException.class);

    // If the Schema is maintained by the entity store, we don't have to import.
    boolean isManagedSchema = isManagedEntity(catalogIdentifier, Capability.Scope.SCHEMA);
    if (isManagedSchema) {
      return EntityCombinedSchema.of(schema)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  schema.properties()))
          // The meta of managed schema is stored by Gravitino, we don't need to import it.
          .withImported(true /* imported */);
    }

    StringIdentifier stringId = getStringIdFromProperties(schema.properties());
    // Case 1: The schema is not created by Gravitino or the external system does not support
    // storing string identifiers.
    if (stringId == null) {
      SchemaEntity schemaEntity = getEntity(ident, SCHEMA, SchemaEntity.class);
      if (schemaEntity == null) {
        return EntityCombinedSchema.of(schema)
            .withHiddenProperties(
                getHiddenPropertyNames(
                    catalogIdentifier,
                    HasPropertyMetadata::schemaPropertiesMetadata,
                    schema.properties()))
            .withImported(false);
      }

      return EntityCombinedSchema.of(schema, schemaEntity)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::schemaPropertiesMetadata,
                  schema.properties()))
          // For some catalogs like PG, the identifier information is not stored in the schema's
          // metadata, we need to check if this schema is existed in the store, if so we don't
          // need to import.
          .withImported(true);
    }

    SchemaEntity schemaEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, SCHEMA, SchemaEntity.class),
            "GET",
            stringId.id());

    return EntityCombinedSchema.of(schema, schemaEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdentifier,
                HasPropertyMetadata::schemaPropertiesMetadata,
                schema.properties()))
        .withImported(schemaEntity != null);
  }
}
