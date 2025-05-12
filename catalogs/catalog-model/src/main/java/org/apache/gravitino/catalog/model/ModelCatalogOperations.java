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
package org.apache.gravitino.catalog.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class ModelCatalogOperations extends ManagedSchemaOperations
    implements CatalogOperations, ModelCatalog {

  private static final int INIT_VERSION = 0;

  private final EntityStore store;

  public ModelCatalogOperations(EntityStore store) {
    this.store = store;
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    // No-op for model catalog.
  }

  @Override
  protected EntityStore store() {
    return store;
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    NamespaceUtil.checkModel(namespace);

    try {
      List<ModelEntity> models = store.list(namespace, ModelEntity.class, Entity.EntityType.MODEL);
      return models.stream()
          .map(m -> NameIdentifier.of(namespace, m.name()))
          .toArray(NameIdentifier[]::new);

    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to list models under namespace " + namespace, ioe);
    }
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    NameIdentifierUtil.checkModel(ident);

    try {
      ModelEntity model = store.get(ident, Entity.EntityType.MODEL, ModelEntity.class);
      return toModelImpl(model);

    } catch (NoSuchEntityException e) {
      throw new NoSuchModelException(e, "Model %s does not exist", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to get model " + ident, ioe);
    }
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws ModelAlreadyExistsException {
    NameIdentifierUtil.checkModel(ident);

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property string identifier should not be null");

    ModelEntity model =
        ModelEntity.builder()
            .withId(stringId.id())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withProperties(properties)
            .withLatestVersion(INIT_VERSION)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(model, false /* overwrite */);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register model " + ident, e);
    } catch (EntityAlreadyExistsException e) {
      throw new ModelAlreadyExistsException(e, "Model %s already exists", ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
    }

    return toModelImpl(model);
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    try {
      return store.delete(ident, Entity.EntityType.MODEL);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete model " + ident, ioe);
    }
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    NameIdentifierUtil.checkModel(ident);
    Namespace modelVersionNs = NamespaceUtil.toModelVersionNs(ident);

    try {
      List<ModelVersionEntity> versions =
          store.list(modelVersionNs, ModelVersionEntity.class, Entity.EntityType.MODEL_VERSION);
      return versions.stream().mapToInt(ModelVersionEntity::version).toArray();

    } catch (NoSuchEntityException e) {
      throw new NoSuchModelException(e, "Model %s does not exist", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to list model versions for model " + ident, ioe);
    }
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, version);

    return internalGetModelVersion(modelVersionIdent);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, alias);

    return internalGetModelVersion(modelVersionIdent);
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    NameIdentifierUtil.checkModel(ident);

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property string identifier should not be null");

    List<String> aliasList = aliases == null ? Lists.newArrayList() : Lists.newArrayList(aliases);
    ModelVersionEntity modelVersion =
        ModelVersionEntity.builder()
            .withModelIdentifier(ident)
            // This version is just a placeholder, it will not be used in the actual model version
            // insert operation, the version will be updated to the latest version of the model when
            // executing the insert operation.
            .withVersion(INIT_VERSION)
            .withAliases(aliasList)
            .withUri(uri)
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(modelVersion, false /* overwrite */);
    } catch (IOException e) {
      throw new RuntimeException("Failed to link model version " + ident, e);
    } catch (EntityAlreadyExistsException e) {
      throw new ModelVersionAliasesAlreadyExistException(
          e, "Model version aliases %s already exist", ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchModelException(e, "Model %s does not exist", ident);
    }
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, version);

    return internalDeleteModelVersion(modelVersionIdent);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, alias);

    return internalDeleteModelVersion(modelVersionIdent);
  }

  /** {@inheritDoc} */
  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    try {
      if (!store.exists(ident, Entity.EntityType.MODEL)) {
        throw new NoSuchModelException("Model %s does not exist", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to alter model " + ident, ioe);
    }

    try {
      ModelEntity updatedModelEntity =
          store.update(
              ident,
              ModelEntity.class,
              Entity.EntityType.MODEL,
              e -> updateModelEntity(ident, e, changes));

      return toModelImpl(updatedModelEntity);

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load model " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchModelException(nsee, "Model %s does not exist", ident);
    } catch (EntityAlreadyExistsException eaee) {
      // This is happened when renaming a model to an existing model name.
      throw new RuntimeException("Model already exist " + ident.name(), eaee);
    }
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, version);

    return internalUpdateModelVersion(modelVersionIdent, changes);
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    NameIdentifierUtil.checkModel(ident);
    NameIdentifier modelVersionIdent = NameIdentifierUtil.toModelVersionIdentifier(ident, alias);

    return internalUpdateModelVersion(modelVersionIdent, changes);
  }

  private ModelEntity updateModelEntity(
      NameIdentifier ident, ModelEntity modelEntity, ModelChange... changes) {

    Map<String, String> entityProperties =
        modelEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(modelEntity.properties());
    String entityName = ident.name();
    String entityComment = modelEntity.comment();
    Long entityId = modelEntity.id();
    AuditInfo entityAuditInfo = modelEntity.auditInfo();
    Namespace entityNamespace = modelEntity.namespace();
    Integer entityLatestVersion = modelEntity.latestVersion();
    String modifier = PrincipalUtils.getCurrentPrincipal().getName();

    for (ModelChange change : changes) {
      if (change instanceof ModelChange.RenameModel) {
        entityName = ((ModelChange.RenameModel) change).newName();

      } else if (change instanceof ModelChange.SetProperty) {
        ModelChange.SetProperty setPropertyChange = (ModelChange.SetProperty) change;
        doSetProperty(entityProperties, setPropertyChange);

      } else if (change instanceof ModelChange.RemoveProperty) {
        ModelChange.RemoveProperty removePropertyChange = (ModelChange.RemoveProperty) change;
        doRemoveProperty(entityProperties, removePropertyChange);

      } else if (change instanceof ModelChange.UpdateComment) {
        entityComment = ((ModelChange.UpdateComment) change).newComment();

      } else {
        throw new IllegalArgumentException(
            "Unsupported model change: " + change.getClass().getSimpleName());
      }
    }

    return ModelEntity.builder()
        .withName(entityName)
        .withId(entityId)
        .withComment(entityComment)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(entityAuditInfo.creator())
                .withCreateTime(entityAuditInfo.createTime())
                .withLastModifier(modifier)
                .withLastModifiedTime(Instant.now())
                .build())
        .withNamespace(entityNamespace)
        .withProperties(entityProperties)
        .withLatestVersion(entityLatestVersion)
        .build();
  }

  private ModelVersion internalUpdateModelVersion(
      NameIdentifier ident, ModelVersionChange... changes) {
    NameIdentifier modelIdent = NameIdentifierUtil.toModelIdentifier(ident);
    try {
      if (!store.exists(modelIdent, Entity.EntityType.MODEL)) {
        throw new NoSuchModelException("Model %s does not exist", modelIdent);
      }

      if (!store.exists(ident, Entity.EntityType.MODEL_VERSION)) {
        throw new NoSuchModelVersionException("Model version %s does not exist", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to alter model version " + ident, ioe);
    }

    try {
      ModelVersionEntity updatedModelVersionEntity =
          store.update(
              ident,
              ModelVersionEntity.class,
              Entity.EntityType.MODEL_VERSION,
              e -> updateModelVersionEntity(e, changes));

      return toModelVersionImpl(updatedModelVersionEntity);

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load model version " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchModelVersionException(nsee, "Model Version %s does not exist", ident);
    }
  }

  private ModelVersionEntity updateModelVersionEntity(
      ModelVersionEntity modelVersionEntity, ModelVersionChange... changes) {
    NameIdentifier entityModelIdentifier = modelVersionEntity.modelIdentifier();
    int entityVersion = modelVersionEntity.version();
    String entityComment = modelVersionEntity.comment();
    List<String> entityAliases =
        modelVersionEntity.aliases() == null
            ? Lists.newArrayList()
            : Lists.newArrayList(modelVersionEntity.aliases());
    String entityUri = modelVersionEntity.uri();
    Map<String, String> entityProperties =
        modelVersionEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(modelVersionEntity.properties());
    AuditInfo entityAuditInfo = modelVersionEntity.auditInfo();
    String modifier = PrincipalUtils.getCurrentPrincipal().getName();

    for (ModelVersionChange change : changes) {
      if (change instanceof ModelVersionChange.UpdateComment) {
        entityComment = ((ModelVersionChange.UpdateComment) change).newComment();

      } else if (change instanceof ModelVersionChange.SetProperty) {
        ModelVersionChange.SetProperty setPropertyChange = (ModelVersionChange.SetProperty) change;
        doSetProperty(entityProperties, setPropertyChange);

      } else if (change instanceof ModelVersionChange.RemoveProperty) {
        ModelVersionChange.RemoveProperty removePropertyChange =
            (ModelVersionChange.RemoveProperty) change;
        doRemoveProperty(entityProperties, removePropertyChange);

      } else if (change instanceof ModelVersionChange.UpdateUri) {
        ModelVersionChange.UpdateUri updateUriChange = (ModelVersionChange.UpdateUri) change;
        entityUri = updateUriChange.newUri();

      } else if (change instanceof ModelVersionChange.UpdateAliases) {
        ModelVersionChange.UpdateAliases updateAliasesChange =
            (ModelVersionChange.UpdateAliases) change;
        Set<String> addTmpSet = updateAliasesChange.aliasesToAdd();
        Set<String> deleteTmpSet = updateAliasesChange.aliasesToRemove();
        Set<String> aliasToAdd = Sets.difference(addTmpSet, deleteTmpSet).immutableCopy();
        Set<String> aliasToDelete = Sets.difference(deleteTmpSet, addTmpSet).immutableCopy();

        doDeleteAlias(entityAliases, aliasToDelete);
        doSetAlias(entityAliases, aliasToAdd);

      } else {
        throw new IllegalArgumentException(
            "Unsupported model version change: " + change.getClass().getSimpleName());
      }
    }

    return ModelVersionEntity.builder()
        .withVersion(entityVersion)
        .withModelIdentifier(entityModelIdentifier)
        .withAliases(entityAliases)
        .withComment(entityComment)
        .withUri(entityUri)
        .withProperties(entityProperties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(entityAuditInfo.creator())
                .withCreateTime(entityAuditInfo.createTime())
                .withLastModifier(modifier)
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private ModelImpl toModelImpl(ModelEntity model) {
    return ModelImpl.builder()
        .withName(model.name())
        .withComment(model.comment())
        .withProperties(model.properties())
        .withLatestVersion(model.latestVersion())
        .withAuditInfo(model.auditInfo())
        .build();
  }

  private ModelVersionImpl toModelVersionImpl(ModelVersionEntity modelVersion) {
    return ModelVersionImpl.builder()
        .withVersion(modelVersion.version())
        .withAliases(modelVersion.aliases().toArray(new String[0]))
        .withUri(modelVersion.uri())
        .withComment(modelVersion.comment())
        .withProperties(modelVersion.properties())
        .withAuditInfo(modelVersion.auditInfo())
        .build();
  }

  private ModelVersion internalGetModelVersion(NameIdentifier ident) {
    try {
      ModelVersionEntity modelVersion =
          store.get(ident, Entity.EntityType.MODEL_VERSION, ModelVersionEntity.class);
      return toModelVersionImpl(modelVersion);

    } catch (NoSuchEntityException e) {
      throw new NoSuchModelVersionException(e, "Model version %s does not exist", ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to get model version " + ident, ioe);
    }
  }

  private boolean internalDeleteModelVersion(NameIdentifier ident) {
    try {
      return store.delete(ident, Entity.EntityType.MODEL_VERSION);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete model version " + ident, ioe);
    }
  }

  private void doRemoveProperty(
      Map<String, String> entityProperties, ModelChange.RemoveProperty change) {
    entityProperties.remove(change.property());
  }

  private void doSetProperty(Map<String, String> entityProperties, ModelChange.SetProperty change) {
    entityProperties.put(change.property(), change.value());
  }

  private void doSetProperty(
      Map<String, String> entityProperties, ModelVersionChange.SetProperty change) {
    entityProperties.put(change.property(), change.value());
  }

  private void doRemoveProperty(
      Map<String, String> entityProperties, ModelVersionChange.RemoveProperty change) {
    entityProperties.remove(change.property());
  }

  private void doDeleteAlias(List<String> entityAliases, Set<String> deleteSet) {
    entityAliases.removeAll(deleteSet);
  }

  private void doSetAlias(List<String> entityAliases, Set<String> addSet) {
    // for fast lookup
    Set<String> aliasSet = new HashSet<>(entityAliases);
    for (String alias : addSet) {
      if (aliasSet.add(alias)) {
        entityAliases.add(alias);
      }
    }
  }
}
