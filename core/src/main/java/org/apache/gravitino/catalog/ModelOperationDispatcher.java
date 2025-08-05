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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.ThrowableFunction;

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
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent, HasPropertyMetadata::modelPropertiesMetadata, model.properties()));
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchModelException, ModelAlreadyExistsException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Map<String, String> updatedProperties =
        checkAndUpdateProperties(
            catalogIdent, properties, HasPropertyMetadata::modelPropertiesMetadata);

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
        .withHiddenProperties(
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
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    return internalListModelVersion(
        ident,
        () ->
            TreeLockUtils.doWithTreeLock(
                ident,
                LockType.READ,
                () ->
                    doWithCatalog(
                        getCatalogIdentifier(ident),
                        c -> c.doWithModelOps(m -> m.listModelVersionInfos(ident)),
                        NoSuchModelException.class)));
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    return internalGetModelVersion(
        ident,
        () ->
            TreeLockUtils.doWithTreeLock(
                ident,
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
                ident,
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
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);
    Map<String, String> updatedProperties =
        checkAndUpdateProperties(
            catalogIdent, properties, HasPropertyMetadata::modelVersionPropertiesMetadata);

    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            doWithCatalog(
                catalogIdent,
                c ->
                    c.doWithModelOps(
                        m -> {
                          m.linkModelVersion(ident, uris, aliases, comment, updatedProperties);
                          return null;
                        }),
                NoSuchModelException.class,
                ModelVersionAliasesAlreadyExistException.class));
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.getModelVersionUri(ident, version, uriName)),
                NoSuchModelVersionException.class,
                NoSuchModelVersionURINameException.class));
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () ->
            doWithCatalog(
                getCatalogIdentifier(ident),
                c -> c.doWithModelOps(m -> m.getModelVersionUri(ident, alias, uriName)),
                NoSuchModelVersionException.class,
                NoSuchModelVersionURINameException.class));
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

  /** {@inheritDoc} */
  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::modelPropertiesMetadata, changes);
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    Model alteredModel =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.WRITE,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithModelOps(f -> f.alterModel(ident, changes)),
                    NoSuchModelException.class,
                    IllegalArgumentException.class));

    return EntityCombinedModel.of(alteredModel)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::modelPropertiesMetadata,
                alteredModel.properties()));
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelVersionException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::modelVersionPropertiesMetadata, changes);
    return executeAlterModelVersion(ident, f -> f.alterModelVersion(ident, version, changes));
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    validateAlterProperties(ident, HasPropertyMetadata::modelVersionPropertiesMetadata, changes);
    return executeAlterModelVersion(ident, f -> f.alterModelVersion(ident, alias, changes));
  }

  private ModelVersion executeAlterModelVersion(
      NameIdentifier ident, ThrowableFunction<ModelCatalog, ModelVersion> fn) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    ModelVersion alteredModelVersion =
        TreeLockUtils.doWithTreeLock(
            ident,
            LockType.WRITE,
            () ->
                doWithCatalog(
                    catalogIdent,
                    c -> c.doWithModelOps(fn),
                    NoSuchModelVersionException.class,
                    IllegalArgumentException.class));

    return EntityCombinedModelVersion.of(alteredModelVersion)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::modelVersionPropertiesMetadata,
                alteredModelVersion.properties()));
  }

  private ModelVersion internalGetModelVersion(
      NameIdentifier ident, Supplier<ModelVersion> supplier) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    ModelVersion modelVersion = supplier.get();
    return EntityCombinedModelVersion.of(modelVersion)
        .withHiddenProperties(
            getHiddenPropertyNames(
                catalogIdent,
                HasPropertyMetadata::modelVersionPropertiesMetadata,
                modelVersion.properties()));
  }

  private ModelVersion[] internalListModelVersion(
      NameIdentifier ident, Supplier<ModelVersion[]> supplier) {
    NameIdentifier catalogIdent = getCatalogIdentifier(ident);

    return Arrays.stream(supplier.get())
        .map(
            v ->
                EntityCombinedModelVersion.of(v)
                    .withHiddenProperties(
                        getHiddenPropertyNames(
                            catalogIdent,
                            HasPropertyMetadata::modelVersionPropertiesMetadata,
                            v.properties())))
        .toArray(ModelVersion[]::new);
  }

  private Map<String, String> checkAndUpdateProperties(
      NameIdentifier catalogIdent,
      Map<String, String> properties,
      Function<HasPropertyMetadata, PropertiesMetadata> propertiesMetadataProvider) {
    TreeLockUtils.doWithTreeLock(
        catalogIdent,
        LockType.READ,
        () ->
            doWithCatalog(
                catalogIdent,
                c ->
                    c.doWithPropertiesMeta(
                        p -> {
                          validatePropertyForCreate(
                              propertiesMetadataProvider.apply(p), properties);
                          return null;
                        }),
                IllegalArgumentException.class));

    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    return StringIdentifier.newPropertiesWithId(stringId, properties);
  }
}
