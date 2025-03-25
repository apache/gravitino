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
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.model.Model;

/**
 * ModelInfo exposes model information for event listener, it's supposed to be read only. Most of
 * the fields are shallow copied internally not deep copies for performance.
 */
@DeveloperApi
public class ModelInfo {
  private final String name;
  private final Map<String, String> properties;
  private final Optional<String> comment;
  private final Optional<Audit> audit;
  private final Optional<Integer> lastVersion;

  /**
   * Constructs a {@link ModelInfo} instance based on a given model.
   *
   * @param model the model to expose information for.
   */
  public ModelInfo(Model model) {
    this(
        model.name(),
        model.properties(),
        model.comment(),
        model.auditInfo(),
        model.latestVersion());
  }

  /**
   * Constructs a {@link ModelInfo} instance based on name, properties, and comment.
   *
   * @param name the name of the model.
   * @param properties the properties of the model.
   * @param comment the comment of the model.
   */
  public ModelInfo(String name, Map<String, String> properties, String comment) {
    this(name, properties, comment, null, null);
  }

  /**
   * Constructs a {@link ModelInfo} instance based on name, properties, comment, audit, and last
   * version.
   *
   * @param name the name of the model.
   * @param properties the properties of the model.
   * @param comment the comment of the model.
   * @param audit the audit information of the model.
   * @param lastVersion the last version of the model.
   */
  public ModelInfo(
      String name,
      Map<String, String> properties,
      String comment,
      Audit audit,
      Integer lastVersion) {
    this.name = name;

    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.comment = Optional.ofNullable(comment);
    this.audit = Optional.ofNullable(audit);
    this.lastVersion = Optional.ofNullable(lastVersion);
  }

  /**
   * Returns the name of the model.
   *
   * @return the name of the model.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the properties of the model.
   *
   * @return the properties of the model.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the comment of the model.
   *
   * @return the comment of the model or null if not set.
   */
  public Optional<String> comment() {
    return comment;
  }

  /**
   * Returns the audit information of the model.
   *
   * @return the audit information of the model or null if not set.
   */
  public Optional<Audit> audit() {
    return audit;
  }

  /**
   * returns the last version of the model.
   *
   * @return the last version of the model, or empty if not set.
   */
  public Optional<Integer> lastVersion() {
    return lastVersion;
  }
}
