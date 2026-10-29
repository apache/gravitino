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
import org.apache.gravitino.annotation.Evolving;

/**
 * An interface representing a single model checkpoint under a model {@link Model}. A model version
 * is a snapshot at a point of time of a model artifact in ML. Users can link a model version to a
 * registered model.
 */
@Evolving
public interface ModelVersion extends Auditable {

  /** The property name for the default uri name of the model version. */
  String PROPERTY_DEFAULT_URI_NAME = "default-uri-name";

  /** The reserved URI name to indicate the URI name is unknown. */
  String URI_NAME_UNKNOWN = "unknown";

  /**
   * The version of this model object. The version number is an integer number starts from 0. Each
   * time the model checkpoint / snapshot is linked to the registered, the version number will be
   * increased by 1.
   *
   * @return The version of the model object.
   */
  int version();

  /**
   * The comment of this model version. This comment can be different from the comment of the model
   * to provide more detailed information about this version.
   *
   * @return The comment of the model version. Null is returned if no comment is set.
   */
  default String comment() {
    return null;
  }

  /**
   * The aliases of this model version. The aliases are the alternative names of the model version.
   * The aliases are optional. The aliases are unique for a model version. If the alias is already
   * set to one model version, it cannot be set to another model version.
   *
   * @return The aliases of the model version.
   */
  String[] aliases();

  /**
   * The unnamed URI of the model artifact. The URI is the location of the model artifact. The URI
   * can be a file path or a remote URI.
   *
   * @return The URI of the model artifact.
   */
  default String uri() {
    return uris().get(URI_NAME_UNKNOWN);
  }

  /**
   * The name and corresponding URI of the model artifact. The key is the name of the URI, and the
   * value is the URI of the model artifact, which can be a file path or a remote URI.
   *
   * <p>The "unknown" URI name is reserved for the compatibility with single URI.
   *
   * @return The URIs of the model version, the key is the name of the URI and the value is the URI
   *     of the model artifact.
   */
  Map<String, String> uris();

  /**
   * The properties of the model version. The properties are key-value pairs that can be used to
   * store additional information of the model version. The properties are optional.
   *
   * @return the properties of the model version.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
