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

import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class ModelOperationDispatcher extends OperationDispatcher implements ModelDispatcher {

  public ModelOperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(namespace.levels()),
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(NameIdentifier.of(namespace.levels())),
                c -> c.doWithModelOps(m -> m.listModels(namespace)),
                NoSuchSchemaException.class));
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Model model =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.READ,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithModelOps(m -> m.getModel(ident)),
                    NoSuchModelException.class));

    return EntityCombinedModel.of(model)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::modelPropertiesMetadata, model.properties()));
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchModelException, ModelAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    TreeLockUtils.doWithTreeLock(
        catalogIdent,
        LockType.READ,
        () ->
            doWithCatalog(
                catalogIdent,
                c ->
                    c.doWithPropertiesMeta(
                        p -> {
                          validatePropertyForCreate(p.filesetPropertiesMetadata(), properties);
                          return null;
                        }),
                IllegalArgumentException.class));

    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    Model registeredModel =
        TreeLockUtils.doWithTreeLock(
            NameIdentifier.of(ident.namespace().levels()),
            LockType.WRITE,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithModelOps(m -> m.registerModel(ident, comment, updatedProperties)),
                    NoSuchSchemaException.class,
                    ModelAlreadyExistsException.class));

    return EntityCombinedModel.of(registeredModel)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::modelPropertiesMetadata,
                registeredModel.properties()));
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(ident.namespace().levels()),
        LockType.WRITE,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.deleteModel(ident)),
                RuntimeException.class));
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.listModelVersions(ident)),
                NoSuchModelException.class));
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    return internalGetModelVersion(
        ident,
        () ->
            TreeLockUtils.doWithTreeLock(
                NameIdentifierUtil.toModelVersionIdentifier(ident, version),
                LockType.READ,
                () ->
                    doWithCatalog(
                        getCatalogIdentifier(ident),
                        c -> c.doWithModelOps(m -> m.getModelVersion(ident, version)),
                        NoSuchModelVersionException.class)));
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    return internalGetModelVersion(
        ident,
        () ->
            TreeLockUtils.doWithTreeLock(
                NameIdentifierUtil.toModelVersionIdentifier(ident, alias),
                LockType.READ,
                () ->
                    doWithCatalog(
                        getCatalogIdentifier(ident),
                        c -> c.doWithModelOps(m -> m.getModelVersion(ident, alias)),
                        NoSuchModelVersionException.class)));
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    TreeLockUtils.doWithTreeLock(
        catalogIdent,
        LockType.READ,
        () ->
            doWithCatalog(
                catalogIdent,
                c ->
                    c.doWithPropertiesMeta(
                        p -> {
                          validatePropertyForCreate(p.filesetPropertiesMetadata(), properties);
                          return null;
                        }),
                IllegalArgumentException.class));

    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Map<String, String> updatedProperties =
        StringIdentifier.newPropertiesWithId(stringId, properties);

    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            doWithCatalog(
                catalogIdent,
                c ->
                    c.doWithModelOps(
                        m -> {
                          m.linkModelVersion(ident, uri, aliases, comment, updatedProperties);
                          return null;
                        }),
                NoSuchModelException.class,
                ModelVersionAliasesAlreadyExistException.class));
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.deleteModelVersion(ident, version)),
                RuntimeException.class));
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.deleteModelVersion(ident, alias)),
                RuntimeException.class));
  }

  private ModelVersion internalGetModelVersion(
      NameIdentifier ident, Supplier<ModelVersion> supplier) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    ModelVersion modelVersion = supplier.get();
    return EntityCombinedModelVersion.of(modelVersion)
        .withHiddenPropertiesSet(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::modelPropertiesMetadata,
                modelVersion.properties()));
  }
}
