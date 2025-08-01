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
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing an ML model under a schema {@link Namespace}. A model is a metadata
 * object that represents the model artifact in ML. Users can register a model object in Gravitino
 * to manage the model metadata. The typical use case is to manage the model in ML lifecycle with a
 * unified way in Gravitino, and access the model artifact with a unified identifier. Also, with the
 * model registered in Gravitino, users can govern the model with Gravitino's unified audit, tag,
 * and role management.
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
   * The latest version of the model object. The latest version is the version number of the latest
   * model checkpoint / snapshot that is linked to the registered model.
   *
   * @return The latest version of the model object.
   */
  int latestVersion();

  /**
   * @return The {@link SupportsTags} if the model supports tag operations.
   * @throws UnsupportedOperationException If the model does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Model does not support tag operations.");
  }

  /**
   * @return The {@link SupportsPolicies} if the model supports policy operations.
   * @throws UnsupportedOperationException If the model does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Model does not support policy operations.");
  }

  /**
   * @return The {@link SupportsRoles} if the model supports role operations.
   * @throws UnsupportedOperationException If the model does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Model does not support role operations.");
  }
}
