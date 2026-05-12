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

import static org.apache.gravitino.Entity.EntityType.VIEW;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;
import static org.apache.gravitino.utils.NameIdentifierUtil.getSchemaIdentifier;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@code ViewOperationDispatcher} is the operation dispatcher for view operations. */
public class ViewOperationDispatcher extends OperationDispatcher implements ViewDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(ViewOperationDispatcher.class);

  /**
   * Creates a new ViewOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for view operations.
   * @param store The EntityStore instance to be used for view operations.
   * @param idGenerator The IdGenerator instance to be used for view operations.
   */
  public ViewOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  /**
   * Lists the views within a schema.
   *
   * @param namespace The namespace of the schema containing the views.
   * @return An array of {@link NameIdentifier} objects representing the identifiers of the views in
   *     the schema.
   * @throws NoSuchSchemaException If the specified schema does not exist.
   */
  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(namespace.levels()),
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
                c -> c.doWithViewOps(v -> v.listViews(namespace)),
                NoSuchSchemaException.class));
  }

  /**
   * Loads a view.
   *
   * @param ident The identifier of the view to load.
   * @return The loaded {@link View} object representing the requested view.
   * @throws NoSuchViewException If the specified view does not exist.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    LOG.info("Loading view: {}", ident);

    EntityCombinedView entityCombinedView =
        TreeLockUtils.doWithTreeLock(ident, LockType.READ, () -> internalLoadView(ident));

    if (!entityCombinedView.imported()) {
      SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      schemaDispatcher.loadSchema(schemaIdent);

      entityCombinedView =
          TreeLockUtils.doWithTreeLock(schemaIdent, LockType.WRITE, () -> importView(ident));
    }

    return entityCombinedView;
  }

  /**
   * Creates a new view in a schema.
   *
   * @param ident The identifier of the view to create.
   * @param comment A description or comment associated with the view.
   * @param columns An array of {@link Column} objects representing the output columns of the view.
   * @param representations An array of {@link Representation} objects representing the SQL
   *     definitions of the view across different dialects.
   * @param defaultCatalog The default catalog to use for unqualified references.
   * @param defaultSchema The default schema to use for unqualified references.
   * @param properties Additional properties to set for the view.
   * @return The newly created {@link View} object.
   * @throws NoSuchSchemaException If the schema in which to create the view does not exist.
   * @throws ViewAlreadyExistsException If a view with the same name already exists in the schema.
   */
  @Override
  public View createView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    Preconditions.checkArgument(
        representations != null && representations.length >= 1,
        "representations must not be null or empty");

    // Load the schema to make sure the schema exists.
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    schemaDispatcher.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        schemaIdent,
        LockType.WRITE,
        () ->
            internalCreateView(
                ident,
                comment,
                columns,
                representations,
                defaultCatalog,
                defaultSchema,
                properties));
  }

  /**
   * Alters an existing view.
   *
   * @param ident The identifier of the view to alter.
   * @param changes An array of {@link ViewChange} objects representing the changes to apply to the
   *     view.
   * @return The altered {@link View} object after applying the changes.
   * @throws NoSuchViewException If the view to alter does not exist.
   * @throws IllegalArgumentException If an unsupported or invalid change is specified.
   */
  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::tablePropertiesMetadata, changes);
    NameIdentifier lockIdent = ident;
    for (ViewChange change : changes) {
      if (change instanceof ViewChange.RenameView) {
        lockIdent = getSchemaIdentifier(ident);
        break;
      }
    }

    NameIdentifier nameIdentifierForLock = lockIdent;
    return TreeLockUtils.doWithTreeLock(
        nameIdentifierForLock,
        nameIdentifierForLock.equals(ident) ? LockType.READ : LockType.WRITE,
        () -> {
          NameIdentifier catalogIdent = getCatalogIdentifier(ident);
          View alteredView =
              doWithCatalog(
                  catalogIdent,
                  c ->
                      c.doWithViewOps(
                          v -> v.alterView(ident, applyCapabilities(c.capabilities(), changes))),
                  NoSuchViewException.class,
                  IllegalArgumentException.class);

          boolean isManagedView = isManagedEntity(catalogIdent, Capability.Scope.VIEW);
          if (isManagedView) {
            return EntityCombinedView.of(alteredView)
                .withHiddenProperties(
                    getHiddenPropertyNames(
                        catalogIdent,
                        HasPropertyMetadata::tablePropertiesMetadata,
                        alteredView.properties()));
          }

          StringIdentifier stringId = getStringIdFromProperties(alteredView.properties());
          // Case 1: The view is not created by Gravitino and this view is never imported.
          ViewEntity existing = null;
          if (stringId == null) {
            existing = getEntity(ident, VIEW, ViewEntity.class);
            if (existing == null) {
              return EntityCombinedView.of(alteredView)
                  .withHiddenProperties(
                      getHiddenPropertyNames(
                          catalogIdent,
                          HasPropertyMetadata::tablePropertiesMetadata,
                          alteredView.properties()));
            }
          }

          long viewId = stringId != null ? stringId.id() : existing.id();
          ViewEntity updatedViewEntity =
              operateOnEntity(
                  ident,
                  id ->
                      store.update(
                          id,
                          ViewEntity.class,
                          VIEW,
                          viewEntity -> applyChangesToEntity(viewEntity, alteredView, changes)),
                  "UPDATE",
                  viewId);

          return EntityCombinedView.of(alteredView, updatedViewEntity)
              .withHiddenProperties(
                  getHiddenPropertyNames(
                      catalogIdent,
                      HasPropertyMetadata::tablePropertiesMetadata,
                      alteredView.properties()));
        });
  }

  /**
   * Drops a view from the catalog.
   *
   * @param ident The identifier of the view to drop.
   * @return {@code true} if the view was successfully dropped, {@code false} if the view does not
   *     exist.
   * @throws RuntimeException If an error occurs while dropping the view.
   */
  @Override
  public boolean dropView(NameIdentifier ident) {
    NameIdentifier schemaIdentifier = getSchemaIdentifier(ident);
    return TreeLockUtils.doWithTreeLock(
        schemaIdentifier,
        LockType.WRITE,
        () -> {
          NameIdentifier catalogIdent = getCatalogIdentifier(ident);
          boolean droppedFromCatalog =
              doWithCatalog(
                  catalogIdent,
                  c -> c.doWithViewOps(v -> v.dropView(ident)),
                  RuntimeException.class);

          boolean isManagedView = isManagedEntity(catalogIdent, Capability.Scope.VIEW);
          if (isManagedView) {
            return droppedFromCatalog;
          }

          // For unmanaged view, it could happen that the view:
          // 1. Is not found in the catalog (dropped directly from underlying sources)
          // 2. Is found in the catalog but not in the store (not managed by Gravitino)
          // 3. Is found in the catalog and the store (managed by Gravitino)
          // 4. Neither found in the catalog nor in the store.
          // In all situations, we try to delete the view from the store, but we don't take the
          // return value of the store operation into account. We only take the return value of the
          // catalog into account.
          try {
            store.delete(ident, VIEW);
          } catch (NoSuchEntityException e) {
            LOG.warn("The view to be dropped does not exist in the store: {}", ident, e);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return droppedFromCatalog;
        });
  }

  private View internalCreateView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties) {
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
    // ViewEntity will be visible.
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    View catalogView =
        doWithCatalog(
            catalogIdent,
            c ->
                c.doWithViewOps(
                    v ->
                        v.createView(
                            ident,
                            comment,
                            columns == null ? new Column[0] : columns,
                            representations,
                            defaultCatalog,
                            defaultSchema,
                            updatedProperties)),
            NoSuchSchemaException.class,
            ViewAlreadyExistsException.class);

    // If the view is managed by Gravitino, we don't need to create ViewEntity and store it again.
    boolean isManagedView = isManagedEntity(catalogIdent, Capability.Scope.VIEW);
    if (isManagedView) {
      return EntityCombinedView.of(catalogView)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  catalogView.properties()));
    }

    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
            .withCreateTime(Instant.now())
            .build();

    ViewEntity viewEntity =
        ViewEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withColumns(columns == null ? new Column[0] : columns)
            .withRepresentations(representations)
            .withDefaultCatalog(defaultCatalog)
            .withDefaultSchema(defaultSchema)
            .withProperties(properties)
            .withAuditInfo(audit)
            .build();

    try {
      store.put(viewEntity, true /* overwrite */);
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      return EntityCombinedView.of(catalogView)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdent,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  catalogView.properties()));
    }

    // Merge both the metadata from catalog operation and the metadata from entity store.
    return EntityCombinedView.of(catalogView, viewEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::tablePropertiesMetadata,
                catalogView.properties()));
  }

  private EntityCombinedView internalLoadView(NameIdentifier ident) throws NoSuchViewException {
    NameIdentifier catalogIdentifier = getCatalogIdentifier(ident);
    View view =
        doWithCatalog(
            catalogIdentifier,
            c -> c.doWithViewOps(v -> v.loadView(ident)),
            NoSuchViewException.class);

    boolean isManagedView = isManagedEntity(catalogIdentifier, Capability.Scope.VIEW);
    if (isManagedView) {
      return EntityCombinedView.of(view)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  view.properties()))
          // The metadata of managed view is stored by Gravitino, so it is always imported.
          .withImported(true /* imported */);
    }

    StringIdentifier stringId = getStringIdFromProperties(view.properties());
    // Case 1: The view is not created by Gravitino or the external system does not support storing
    // string identifier.
    if (stringId == null) {
      ViewEntity viewEntity = getEntity(ident, VIEW, ViewEntity.class);
      if (viewEntity == null) {
        return EntityCombinedView.of(view)
            .withHiddenProperties(
                getHiddenPropertyNames(
                    catalogIdentifier,
                    HasPropertyMetadata::tablePropertiesMetadata,
                    view.properties()))
            // Some views don't have properties or are not created by Gravitino,
            // we can't use stringIdentifier to judge whether view is ever imported or not.
            // We need to check whether the entity exists.
            .withImported(false);
      }

      return EntityCombinedView.of(view, viewEntity)
          .withHiddenProperties(
              getHiddenPropertyNames(
                  catalogIdentifier,
                  HasPropertyMetadata::tablePropertiesMetadata,
                  view.properties()))
          // For some catalogs, the identifier information is not stored in the view's
          // metadata, we need to check if this view exists in the store, if so we don't
          // need to import.
          .withImported(true);
    }

    ViewEntity viewEntity =
        operateOnEntity(
            ident,
            identifier -> store.get(identifier, VIEW, ViewEntity.class),
            "GET",
            stringId.id());

    return EntityCombinedView.of(view, viewEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdentifier, HasPropertyMetadata::tablePropertiesMetadata, view.properties()))
        .withImported(viewEntity != null);
  }

  private EntityCombinedView importView(NameIdentifier ident) throws NoSuchViewException {
    EntityCombinedView entityCombinedView = internalLoadView(ident);

    if (entityCombinedView.imported()) {
      return entityCombinedView;
    }

    StringIdentifier stringId =
        getStringIdFromProperties(entityCombinedView.viewFromCatalog().properties());

    long uid;
    if (stringId != null) {
      // If the entity in the store doesn't match the external system, we use the data
      // of external system to correct it.
      LOG.warn(
          "The View uid {} existed but still need to be imported, this could happen "
              + "when View is renamed by external systems not controlled by Gravitino. In this "
              + "case, we need to overwrite the stored entity to keep the consistency.",
          stringId);
      uid = stringId.id();
    } else {
      // If entity doesn't exist, we import the entity from the external system.
      uid = idGenerator.nextId();
    }

    View catalogView = entityCombinedView.viewFromCatalog();
    AuditInfo audit =
        AuditInfo.builder()
            .withCreator(catalogView.auditInfo().creator())
            .withCreateTime(catalogView.auditInfo().createTime())
            .withLastModifier(catalogView.auditInfo().lastModifier())
            .withLastModifiedTime(catalogView.auditInfo().lastModifiedTime())
            .build();
    ViewEntity viewEntity =
        ViewEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(catalogView.comment())
            .withColumns(catalogView.columns() == null ? new Column[0] : catalogView.columns())
            .withRepresentations(
                catalogView.representations() == null
                    ? new Representation[0]
                    : catalogView.representations())
            .withDefaultCatalog(catalogView.defaultCatalog())
            .withDefaultSchema(catalogView.defaultSchema())
            .withProperties(catalogView.properties())
            .withAuditInfo(audit)
            .build();
    try {
      store.put(viewEntity, true /* overwrite */);
    } catch (EntityAlreadyExistsException e) {
      LOG.error("Failed to import view {} with id {} to the store.", ident, uid, e);
      throw new UnsupportedOperationException(
          "View managed by multiple catalogs. This may cause unexpected issues such as privilege conflicts. "
              + "To resolve: Remove all catalogs managing this view, then recreate one catalog to ensure single-catalog management.");
    } catch (Exception e) {
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, "put", ident, e);
      throw new RuntimeException("Fail to import the view entity to the store.", e);
    }

    return EntityCombinedView.of(catalogView, viewEntity)
        .withHiddenProperties(
            getHiddenPropertyNames(
                getCatalogIdentifier(ident),
                HasPropertyMetadata::tablePropertiesMetadata,
                catalogView.properties()))
        .withImported(true);
  }

  private ViewEntity applyChangesToEntity(
      ViewEntity current, View alteredView, ViewChange[] changes) {
    String name = alteredView.name();
    String comment = alteredView.comment();
    String defaultCatalog = alteredView.defaultCatalog();
    String defaultSchema = alteredView.defaultSchema();
    Map<String, String> properties =
        current.properties() == null ? new HashMap<>() : new HashMap<>(current.properties());

    for (ViewChange change : changes) {
      if (change instanceof ViewChange.SetProperty) {
        ViewChange.SetProperty sp = (ViewChange.SetProperty) change;
        properties.put(sp.getProperty(), sp.getValue());
      } else if (change instanceof ViewChange.RemoveProperty) {
        properties.remove(((ViewChange.RemoveProperty) change).getProperty());
      } else if (!(change instanceof ViewChange.RenameView)
          && !(change instanceof ViewChange.ReplaceView)) {
        throw new IllegalArgumentException("Unsupported view change: " + change);
      }
    }

    Namespace namespace = current.namespace();

    AuditInfo newAudit =
        AuditInfo.builder()
            .withCreator(current.auditInfo().creator())
            .withCreateTime(current.auditInfo().createTime())
            .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
            .withLastModifiedTime(Instant.now())
            .build();

    return ViewEntity.builder()
        .withId(current.id())
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withColumns(
            alteredView.columns() == null
                ? new Column[0]
                : Arrays.copyOf(alteredView.columns(), alteredView.columns().length))
        .withRepresentations(
            Arrays.copyOf(alteredView.representations(), alteredView.representations().length))
        .withDefaultCatalog(defaultCatalog)
        .withDefaultSchema(defaultSchema)
        .withProperties(properties)
        .withAuditInfo(newAudit)
        .build();
  }
}
