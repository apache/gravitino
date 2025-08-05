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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.model.ModelVersion;

/**
 * {@link ModelVersionInfo} exposes model version information for event listener, it's supposed to
 * be read only. Most of the fields are shallow copied internally not deep copies for performance.
 */
@DeveloperApi
public class ModelVersionInfo {
  private final Map<String, String> uris;
  private final Map<String, String> properties;

  private final Optional<String[]> aliases;
  private final Optional<String> comment;
  private final Optional<Audit> audit;

  /**
   * Constructs a {@link ModelVersionInfo} instance based on a given {@link ModelVersion}.
   *
   * @param modelVersion the model version to expose information for.
   */
  public ModelVersionInfo(ModelVersion modelVersion) {
    this(
        modelVersion.uris(),
        modelVersion.comment(),
        modelVersion.properties(),
        modelVersion.aliases(),
        modelVersion.auditInfo());
  }

  /**
   * Constructs a {@link ModelVersionInfo} instance based on given URI, comment, properties, and
   * aliases.
   *
   * @param uri The URI of the model version.
   * @param comment The comment of the model version.
   * @param properties The properties of the model version.
   * @param aliases The aliases of the model version.
   */
  public ModelVersionInfo(
      String uri, String comment, Map<String, String> properties, String[] aliases) {
    this(uri, comment, properties, aliases, null);
  }

  /**
   * Constructs a {@link ModelVersionInfo} instance based on given URI, comment, properties,
   * aliases, and audit information.
   *
   * @param uri The URI of the model version.
   * @param comment The comment of the model version.
   * @param properties The properties of the model version.
   * @param aliases The aliases of the model version.
   * @param auditInfo The audit information of the model version.
   */
  public ModelVersionInfo(
      String uri,
      String comment,
      Map<String, String> properties,
      String[] aliases,
      Audit auditInfo) {
    this.uris =
        uri == null ? Collections.emptyMap() : ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, uri);
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.comment = Optional.ofNullable(comment);
    this.audit = Optional.ofNullable(auditInfo);
    this.aliases = Optional.ofNullable(aliases);
  }

  /**
   * Constructs a {@link ModelVersionInfo} instance based on given URIs, comment, properties,
   * aliases, and audit information.
   *
   * @param uris The URIs of the model version.
   * @param comment The comment of the model version.
   * @param properties The properties of the model version.
   * @param aliases The aliases of the model version.
   * @param auditInfo The audit information of the model version.
   */
  public ModelVersionInfo(
      Map<String, String> uris,
      String comment,
      Map<String, String> properties,
      String[] aliases,
      Audit auditInfo) {
    this.uris = uris;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.comment = Optional.ofNullable(comment);
    this.audit = Optional.ofNullable(auditInfo);
    this.aliases = Optional.ofNullable(aliases);
  }

  /**
   * Returns the unknown URI of the model version.
   *
   * @return the unknown URI of the model version.
   */
  public String uri() {
    return uris.get(ModelVersion.URI_NAME_UNKNOWN);
  }

  /**
   * Returns the URIs of the model version.
   *
   * @return the URIs of the model version.
   */
  public Map<String, String> uris() {
    return uris;
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
    return audit;
  }
}
