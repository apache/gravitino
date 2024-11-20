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

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing an ML model under a schema {@link Namespace}. A model is a metadata
 * object that represents the model artifact in ML. Users can register a model object in Gravitino
 * to manage the mode metadata. The typical use case is to manage the model in ML lifecycle with a
 * unified way in Gravitino, and access the model artifact with a unified identifier. Also, with the
 * model registered in Gravitino, users can also govern the model with Gravitino's unified audit,
 * tag, and role management.
 *
 * <p>The difference of Model and tabular data is that the model is schema-free, and the main
 * property of the model is the model artifact URL. The difference compared to the fileset is that
 * the model is versioned, and the model object contains the version information.
 */
@Evolving
public interface Model extends Auditable {

  /** @return Name of the model object. */
  String name();

  /**
   * The comment of the model object. This is the general description of the model object. User can
   * still add more detailed information in the model version.
   *
   * @return The comment of the model object. Null is returned if no comment is set.
   */
  default String comment() {
    return null;
  }

  /**
   * The properties of the model object. The properties are key-value pairs that can be used to
   * store additional information of the model object. The properties are optional.
   *
   * <p>Users can still specify the properties in the model version for different information.
   *
   * @return the properties of the model object.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * The versions of the model object. The model object can have multiple versions. Each version
   * contains the detailed information of the model artifact, like the URL, the training properties,
   * etc.
   *
   * @return The versions of the model object.
   */
  ModelVersion[] versions();

  /**
   * Get the model version by the version number. If the version does not exist, throw an exception.
   *
   * @param version The version number of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException if the version does not exist.
   */
  ModelVersion version(int version) throws NoSuchModelVersionException;

  /**
   * Get the model version by the version alias. If the version does not exist, throw an exception.
   *
   * @param alias The version alias of the model.
   * @return The model version object.
   * @throws NoSuchModelVersionException if the version does not exist.
   */
  ModelVersion versionAlias(String alias) throws NoSuchModelVersionException;

  /**
   * Check if the model version exists by the version number.
   *
   * @param version The version number of the model.
   * @return True if the version exists, false otherwise.
   */
  default boolean versionExists(int version) {
    try {
      version(version);
      return true;
    } catch (NoSuchModelVersionException e) {
      return false;
    }
  }

  /**
   * Check if the model version exists by the version alias.
   *
   * @param alias The version alias of the model.
   * @return True if the version exists, false otherwise.
   */
  default boolean versionExists(String alias) {
    try {
      versionAlias(alias);
      return true;
    } catch (NoSuchModelVersionException e) {
      return false;
    }
  }

  /**
   * Link a new version model to the registered model object. The new version model will be added to
   * the model object. If the model object does not exist, it will throw an exception.
   *
   * @param aliases The aliases of the model version. The aliases should be unique in this model,
   *     otherwise the {@link ModelVersionAliasesAlreadyExistsException} will be thrown. The aliases
   *     are optional and can be empty.
   * @param comment The comment of the model version. The comment is optional and can be null.
   * @param uri The model artifact URI.
   * @param properties The properties of the model version. The properties are optional and can be
   *     null or empty.
   * @return The created model version object.
   * @throws NoSuchModelException If the model does not exist.
   * @throws ModelVersionAliasesAlreadyExistsException If the aliases already exist in the model.
   */
  ModelVersion link(String[] aliases, String comment, String uri, Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistsException;

  /**
   * Remove the model version object of a model by the version number. If the version does not
   * exist, it will return false.
   *
   * @param version The version number of the model.
   * @return True if the version is removed, false if the version does not exist.
   */
  boolean deleteVersion(int version);

  /**
   * Remove the model version object of a model by the version alias. If the version does not exist,
   * it will return false.
   *
   * @param alias The version alias of the model.
   * @return True if the version is removed, false if the version does not exist.
   */
  boolean deleteVersion(String alias);

  /**
   * @return The {@link SupportsTags} if the model supports tag operations.
   * @throws UnsupportedOperationException If the model does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Model does not support tag operations.");
  }

  /**
   * @return The {@link SupportsRoles} if the model supports role operations.
   * @throws UnsupportedOperationException If the model does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Model does not support role operations.");
  }
}
