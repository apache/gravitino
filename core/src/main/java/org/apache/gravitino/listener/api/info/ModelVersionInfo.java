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

package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.model.ModelVersion;

/**
 * {@link ModelVersionInfo} exposes model version information for event listener, it's supposed to
 * be read only. Most of the fields are shallow copied internally not deep copies for performance.
 */
public class ModelVersionInfo {
  private final String uri;
  private final Map<String, String> properties;

  private final Optional<String[]> aliases;
  private final Optional<String> comment;
  private final Optional<Audit> auditInfo;

  /**
   * Constructs model version information based on a given {@link ModelVersion}.
   *
   * @param modelVersion the model version to expose information for.
   */
  public ModelVersionInfo(ModelVersion modelVersion) {
    this(
        modelVersion.uri(),
        modelVersion.comment(),
        modelVersion.properties(),
        modelVersion.aliases(),
        modelVersion.auditInfo());
  }

  public ModelVersionInfo(
      String uri, String comment, Map<String, String> properties, String[] aliases) {
    this(uri, comment, properties, aliases, null);
  }

  /**
   * Constructs model version information based on a given arguments.
   *
   * @param uri
   * @param aliases
   * @param comment
   * @param properties
   */
  public ModelVersionInfo(
      String uri,
      String comment,
      Map<String, String> properties,
      String[] aliases,
      Audit auditInfo) {
    this.uri = uri;

    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.comment = Optional.ofNullable(comment);
    this.auditInfo = Optional.ofNullable(auditInfo);
    this.aliases = Optional.ofNullable(aliases);
  }

  /**
   * Returns the URI of the model version.
   *
   * @return the URI of the model version.
   */
  public String uri() {
    return uri;
  }

  /**
   * Returns the properties associated with the model version.
   *
   * @return Map of table properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the aliases of the model version.
   *
   * @return the aliases of the model version (a {@code Optional<String[]>} instance) or null if not
   *     set.
   */
  public Optional<String[]> aliases() {
    return aliases;
  }

  /**
   * Returns the comment of the model version.
   *
   * @return the comment of the model version or null if not set.
   */
  public Optional<String> comment() {
    return comment;
  }

  /**
   * Returns the audit information of the model version.
   *
   * @return the audit information of the model version or null if not set.
   */
  public Optional<Audit> audit() {
    return auditInfo;
  }
}
