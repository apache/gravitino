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
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.ErrorMessages;
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
      throw new NoSuchSchemaException(e, ErrorMessages.SCHEMA_DOES_NOT_EXIST, namespace);
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(ErrorMessages.FAILED_TO_LIST_MODELS, namespace), ioe);
    }
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    NameIdentifierUtil.checkModel(ident);

    try {
      ModelEntity model = store.get(ident, Entity.EntityType.MODEL, ModelEntity.class);
      return toModelImpl(model);

    } catch (NoSuchEntityException e) {
      throw new NoSuchModelException(e, ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
    } catch (IOException ioe) {
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_LOAD_MODEL, ident), ioe);
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
      /* overwrite */
      store.put(model, false);
    } catch (IOException e) {
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_REGISTER_MODEL, ident), e);
    } catch (EntityAlreadyExistsException e) {
      throw new ModelAlreadyExistsException(e, ErrorMessages.MODEL_ALREADY_EXISTS, ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, ErrorMessages.SCHEMA_DOES_NOT_EXIST, ident.namespace());
    }

    return toModelImpl(model);
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    NameIdentifierUtil.checkModel(ident);

    try {
      return store.delete(ident, Entity.EntityType.MODEL);
    } catch (IOException ioe) {
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_DELETE_MODEL, ident), ioe);
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
      throw new NoSuchModelException(e, ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(ErrorMessages.FAILED_TO_LIST_MODEL_VERSIONS, ident), ioe);
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
    Preconditions.checkArgument(stringId != null, ErrorMessages.PROPERTY_NOT_NULL);

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
      throw new RuntimeException(
          String.format(ErrorMessages.FAILED_TO_LINK_MODEL_VERSION, ident), e);
    } catch (EntityAlreadyExistsException e) {
      throw new ModelVersionAliasesAlreadyExistException(
          e, ErrorMessages.MODEL_VERSION_ALREADY_EXISTS, ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchModelException(e, ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
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
        throw new NoSuchModelException(ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_LOAD_MODEL, ident), ioe);
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
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_LOAD_MODEL, ident), ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchModelException(nsee, ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
    } catch (EntityAlreadyExistsException eaee) {
      // This is happened when renaming a model to an existing model name.
      throw new RuntimeException(
          String.format(ErrorMessages.MODEL_DOES_NOT_EXIST, ident.name()), eaee);
    }
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

    for (ModelChange change : changes) {
      if (change instanceof ModelChange.RenameModel) {
        entityName = ((ModelChange.RenameModel) change).newName();
      } else {
        throw new IllegalArgumentException(
            "Unsupported model change: " + change.getClass().getSimpleName());
      }
    }

    return ModelEntity.builder()
        .withName(entityName)
        .withId(entityId)
        .withComment(entityComment)
        .withAuditInfo(entityAuditInfo)
        .withNamespace(entityNamespace)
        .withProperties(entityProperties)
        .withLatestVersion(entityLatestVersion)
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
      throw new NoSuchModelVersionException(e, ErrorMessages.MODEL_DOES_NOT_EXIST, ident);
    } catch (IOException ioe) {
      throw new RuntimeException(String.format(ErrorMessages.FAILED_TO_GET_MODEL, ident), ioe);
    }
  }

  private boolean internalDeleteModelVersion(NameIdentifier ident) {
    try {
      return store.delete(ident, Entity.EntityType.MODEL_VERSION);
    } catch (IOException ioe) {
      throw new RuntimeException(
          String.format(ErrorMessages.FAILED_TO_DELETE_MODEL_VERSION, ident), ioe);
    }
  }
}
