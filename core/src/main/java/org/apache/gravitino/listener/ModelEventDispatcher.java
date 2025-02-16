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
import org.apache.gravitino.listener.api.event.DeleteModelPreEvent;
import org.apache.gravitino.listener.api.event.DeleteModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.GetModelPreEvent;
import org.apache.gravitino.listener.api.event.GetModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.LinkModelVersionPreEvent;
import org.apache.gravitino.listener.api.event.ListModelPreEvent;
import org.apache.gravitino.listener.api.event.ListModelVersionsPreEvent;
import org.apache.gravitino.listener.api.event.RegisterModelPreEvent;
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
    ModelInfo registerRequest = new ModelInfo(ident.name(), properties, comment, null);
    eventBus.dispatchEvent(
        new RegisterModelPreEvent(PrincipalUtils.getCurrentUserName(), ident, registerRequest));
    try {
      Model model = dispatcher.registerModel(ident, comment, properties);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(new GetModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      Model model = dispatcher.getModel(ident);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(new DeleteModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean isExists = dispatcher.deleteModel(ident);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(new ListModelPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      NameIdentifier[] models = dispatcher.listModels(namespace);
      // TODO: ModelEvent
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
    Model model = dispatcher.getModel(ident);
    ModelVersionInfo modelVersionInfo = new ModelVersionInfo(uri, aliases, comment, properties);
    ModelInfo linkRequest = new ModelInfo(model, new ModelVersionInfo[] {modelVersionInfo});
    eventBus.dispatchEvent(
        new LinkModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident, linkRequest));
    try {
      dispatcher.linkModelVersion(ident, uri, aliases, comment, properties);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(new GetModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, version);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(new GetModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      ModelVersion modelVersion = dispatcher.getModelVersion(ident, alias);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(
        new DeleteModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, version);
      // TODO: ModelEvent
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
    eventBus.dispatchEvent(
        new DeleteModelVersionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean isExists = dispatcher.deleteModelVersion(ident, alias);
      // TODO: ModelEvent
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
    Model model = dispatcher.getModel(ident);
    ModelInfo modelInfo = new ModelInfo(model);
    eventBus.dispatchEvent(
        new ListModelVersionsPreEvent(PrincipalUtils.getCurrentUserName(), ident, modelInfo));
    try {
      int[] versions = dispatcher.listModelVersions(ident);
      // TODO: ModelEvent
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
}
