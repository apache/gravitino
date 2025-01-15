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

package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.listener.api.event.DeleteModelEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionEvent;
import org.apache.gravitino.listener.api.event.GetModelEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionEvent;
import org.apache.gravitino.listener.api.event.ListModelEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionsEvent;
import org.apache.gravitino.listener.api.event.RegisterModelEvent;
import org.apache.gravitino.listener.api.info.ModelInfo;
import org.apache.gravitino.listener.api.info.ModelVersionInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code ModelEventDispatcher} is a decorator for {@link ModelDispatcher} that not only delegates
 * model operations to the underlying catalog dispatcher but also dispatches corresponding events to
 * an {@link org.apache.gravitino.listener.EventBus} after each operation is completed. This allows
 * for event-driven workflows or monitoring of model operations.
 */
public class ModelEventDispatcher implements ModelDispatcher {
  private final EventBus eventBus;
  private final ModelDispatcher dispatcher;

  /**
   * Constructs a {@link ModelEventDispatcher} with a specified EventBus and {@link
   * ModelDispatcher}.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link ModelDispatcher} that will perform the actual model
   *     operations.
   */
  public ModelEventDispatcher(EventBus eventBus, ModelDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  /**
   * Register a model in the catalog if the model is not existed, otherwise the {@link
   * ModelAlreadyExistsException} will be thrown. The {@link Model} object will be created when the
   * model is registered, users can call {@link ModelCatalog#linkModelVersion(NameIdentifier,
   * String, String[], String, Map)} to link the model version to the registered {@link Model}.
   *
   * @param ident The name identifier of the model.
   * @param comment The comment of the model. The comment is optional and can be null.
   * @param properties The properties of the model. The properties are optional and can be null or
   *     empty.
   * @return The registered model object.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws ModelAlreadyExistsException If the model already registered.
   */
  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    // TODO: preEvent
    try {
      Model model = dispatcher.registerModel(ident, comment, properties);
      ModelInfo modelInfo = new ModelInfo(model);
      eventBus.dispatchEvent(
          new RegisterModelEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
      return model;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Register a model in the catalog if the model is not existed, otherwise the {@link
   * ModelAlreadyExistsException} will be thrown. The {@link Model} object will be created when the
   * model is registered, in the meantime, the model version (version 0) will also be created and
   * linked to the registered model.
   *
   * @param ident The name identifier of the model.
   * @param uri The model artifact URI.
   * @param aliases The aliases of the model version. The aliases should be unique in this model,
   *     otherwise the {@link ModelVersionAliasesAlreadyExistException} will be thrown. The aliases
   *     are optional and can be empty. Also, be aware that the alias cannot be a number or a number
   *     string.
   * @param comment The comment of the model. The comment is optional and can be null.
   * @param properties The properties of the model. The properties are optional and can be null or
   *     empty.
   * @return The registered model object.
   * @throws NoSuchSchemaException If the schema does not exist when register a model.
   * @throws ModelAlreadyExistsException If the model already registered.
   * @throws ModelVersionAliasesAlreadyExistException If the aliases already exist in the model.
   */
  @Override
  public Model registerModel(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException,
          ModelVersionAliasesAlreadyExistException {
    return dispatcher.registerModel(ident, uri, aliases, comment, properties);
  }

  /**
   * Get a model metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return The model metadata.
   * @throws NoSuchModelException If the model does not exist.
   */
  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    // TODO: preEvent
    try {
      Model model = dispatcher.getModel(ident);
      ModelInfo modelInfo = new ModelInfo(model);
      eventBus.dispatchEvent(
          new GetModelEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
      return model;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Delete the model from the catalog. If the model does not exist, return false. Otherwise, return
   * true. The deletion of the model will also delete all the model versions linked to this model.
   *
   * @param ident The name identifier of the model.
   * @return True if the model is deleted, false if the model does not exist.
   */
  @Override
  public boolean deleteModel(NameIdentifier ident) {
    // TODO: preEvent
    try {
      boolean isExists = dispatcher.deleteModel(ident);
      eventBus.dispatchEvent(
          new DeleteModelEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * List the models in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of model identifiers in the namespace.
   * @throws NoSuchModelException If the model does not exist.
   */
  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    // TODO: preEvent
    try {
      NameIdentifier[] models = dispatcher.listModels(namespace);
      eventBus.dispatchEvent(new ListModelEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return models;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Link a new model version to the registered model object. The new model version will be added to
   * the model object. If the model object does not exist, it will throw an exception. If the
   * version alias already exists in the model, it will throw an exception.
   *
   * @param ident The name identifier of the model.
   * @param uri The URI of the model version artifact.
   * @param aliases The aliases of the model version. The aliases should be unique in this model,
   *     otherwise the {@link ModelVersionAliasesAlreadyExistException} will be thrown. The aliases
   *     are optional and can be empty. Also, be aware that the alias cannot be a number or a number
   *     string.
   * @param comment The comment of the model version. The comment is optional and can be null.
   * @param properties The properties of the model version. The properties are optional and can be
   *     null or empty.
   * @throws NoSuchModelException If the model does not exist.
   * @throws ModelVersionAliasesAlreadyExistException If the aliases already exist in the model.
   */
  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    // TODO: preEvent
    try {
      Model model = dispatcher.getModel(ident);
      ModelVersionInfo modelVersionInfo = new ModelVersionInfo(uri, aliases, comment, properties);
      ModelInfo modelInfo = new ModelInfo(model, new ModelVersionInfo[] {modelVersionInfo});
      dispatcher.linkModelVersion(ident, uri, aliases, comment, properties);
      eventBus.dispatchEvent(
          new LinkModelVersionEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Get a model version by the {@link NameIdentifier} and version number from the catalog.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException If the model version does not exist.
   */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    // TODO: preEvent
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, version);
      ModelInfo modelInfo = getModelInfo(ident, version);
      eventBus.dispatchEvent(
          new GetModelVersionEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
      return modelVersion;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Get a model version by the {@link NameIdentifier} and version alias from the catalog.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException If the model version does not exist.
   */
  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    // TODO: preEvent
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, alias);
      ModelInfo modelInfo = getModelInfo(ident, alias);
      eventBus.dispatchEvent(
          new GetModelVersionEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
      return modelVersion;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Delete the model version by the {@link NameIdentifier} and version number. If the model version
   * does not exist, return false. If the model version is deleted, return true.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return True if the model version is deleted, false if the model version does not exist.
   */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    // TODO: preEvent
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, version);
      eventBus.dispatchEvent(
          new DeleteModelVersionEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Delete the model version by the {@link NameIdentifier} and version alias. If the model version
   * does not exist, return false. If the model version is deleted, return true.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return True if the model version is deleted, false if the model version does not exist.
   */
  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    // TODO: preEvent
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, alias);
      eventBus.dispatchEvent(
          new DeleteModelVersionEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * List all the versions of the register model by {@link NameIdentifier} in the catalog.
   *
   * @param ident The name identifier of the model.
   * @return An array of version numbers of the model.
   * @throws NoSuchModelException If the model does not exist.
   */
  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    // TODO: preEvent
    try {
      int[] versions = dispatcher.listModelVersions(ident);
      Model model = dispatcher.getModel(ident);
      ModelInfo modelInfo = new ModelInfo(model);
      eventBus.dispatchEvent(
          new ListModelVersionsEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
      return versions;
    } catch (Exception e) {
      // TODO: failureEvent
      throw e;
    }
  }

  /**
   * Check if a model exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return true If the model exists, false if the model does not exist.
   */
  @Override
  public boolean modelExists(NameIdentifier ident) {
    return dispatcher.modelExists(ident);
  }

  /**
   * Check if the model version exists by the {@link NameIdentifier} and version number. If the
   * model version exists, return true, otherwise return false.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return True if the model version exists, false if the model version does not exist.
   */
  @Override
  public boolean modelVersionExists(NameIdentifier ident, int version) {
    return dispatcher.modelVersionExists(ident, version);
  }

  /**
   * Check if the model version exists by the {@link NameIdentifier} and version alias. If the model
   * version exists, return true, otherwise return false.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return True if the model version exists, false if the model version does not exist.
   */
  @Override
  public boolean modelVersionExists(NameIdentifier ident, String alias) {
    return dispatcher.modelVersionExists(ident, alias);
  }

  private ModelInfo getModelInfo(NameIdentifier modelIdent, int version) {
    Model model = getModel(modelIdent);
    ModelVersion modelVersion = dispatcher.getModelVersion(modelIdent, version);
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);

    return new ModelInfo(model, new ModelVersionInfo[] {modelVersionInfo});
  }

  private ModelInfo getModelInfo(NameIdentifier modelIdent, String alias) {
    Model model = getModel(modelIdent);
    ModelVersion modelVersion = dispatcher.getModelVersion(modelIdent, alias);
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(modelVersion);

    return new ModelInfo(model, new ModelVersionInfo[] {modelVersionInfo});
  }
}
