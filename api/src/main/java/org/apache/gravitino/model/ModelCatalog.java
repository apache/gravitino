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
package org.apache.gravitino.model;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;

/**
 * The ModelCatalog interface defines the public API for managing model objects in a schema. If the
 * catalog implementation supports model objects, it should implement this interface.
 */
@Evolving
public interface ModelCatalog {

  /**
   * List the models in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of model identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Get a model metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return The model metadata.
   * @throws NoSuchModelException If the model does not exist.
   */
  Model getModel(NameIdentifier ident) throws NoSuchModelException;

  /**
   * Check if a model exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A model identifier.
   * @return true If the model exists, false if the model does not exist.
   */
  default boolean modelExists(NameIdentifier ident) {
    try {
      getModel(ident);
      return true;
    } catch (NoSuchModelException e) {
      return false;
    }
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
  Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException;

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
  default Model registerModel(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException,
          ModelVersionAliasesAlreadyExistException {
    Model model = registerModel(ident, comment, properties);
    linkModelVersion(ident, uri, aliases, comment, properties);
    return model;
  }

  /**
   * Delete the model from the catalog. If the model does not exist, return false. Otherwise, return
   * true. The deletion of the model will also delete all the model versions linked to this model.
   *
   * @param ident The name identifier of the model.
   * @return True if the model is deleted, false if the model does not exist.
   */
  boolean deleteModel(NameIdentifier ident);

  /**
   * List all the versions of the register model by {@link NameIdentifier} in the catalog.
   *
   * @param ident The name identifier of the model.
   * @return An array of version numbers of the model.
   * @throws NoSuchModelException If the model does not exist.
   */
  int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException;

  /**
   * Get a model version by the {@link NameIdentifier} and version number from the catalog.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException If the model version does not exist.
   */
  ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException;

  /**
   * Get a model version by the {@link NameIdentifier} and version alias from the catalog.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException If the model version does not exist.
   */
  ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException;

  /**
   * Check if the model version exists by the {@link NameIdentifier} and version number. If the
   * model version exists, return true, otherwise return false.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return True if the model version exists, false if the model version does not exist.
   */
  default boolean modelVersionExists(NameIdentifier ident, int version) {
    try {
      getModelVersion(ident, version);
      return true;
    } catch (NoSuchModelVersionException e) {
      return false;
    }
  }

  /**
   * Check if the model version exists by the {@link NameIdentifier} and version alias. If the model
   * version exists, return true, otherwise return false.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return True if the model version exists, false if the model version does not exist.
   */
  default boolean modelVersionExists(NameIdentifier ident, String alias) {
    try {
      getModelVersion(ident, alias);
      return true;
    } catch (NoSuchModelVersionException e) {
      return false;
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
  void linkModelVersion(
      NameIdentifier ident,
      String uri,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException;

  /**
   * Delete the model version by the {@link NameIdentifier} and version number. If the model version
   * does not exist, return false. If the model version is deleted, return true.
   *
   * @param ident The name identifier of the model.
   * @param version The version number of the model.
   * @return True if the model version is deleted, false if the model version does not exist.
   */
  boolean deleteModelVersion(NameIdentifier ident, int version);

  /**
   * Delete the model version by the {@link NameIdentifier} and version alias. If the model version
   * does not exist, return false. If the model version is deleted, return true.
   *
   * @param ident The name identifier of the model.
   * @param alias The version alias of the model.
   * @return True if the model version is deleted, false if the model version does not exist.
   */
  boolean deleteModelVersion(NameIdentifier ident, String alias);
}
