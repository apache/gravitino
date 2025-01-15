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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.model.Model;

/**
 * ModelInfo exposes model information for event listener, it's supposed to be read only. Most of
 * the fields are shallow copied internally not deep copies for performance.
 */
@DeveloperApi
public class ModelInfo {
  @Getter private final String name;
  @Nullable private final String comment;
  @Getter private final Map<String, String> properties;
  @Nullable private final Audit audit;
  @Getter private final int lastVersion;
  private final ModelVersionInfo[] versions;

  /**
   * Constructs model information based on a given model.
   *
   * @param model the model to expose information for.
   */
  public ModelInfo(Model model) {
    this(model, null);
  }

  /**
   * Constructs model information based on a given model and model versions.
   *
   * @param model the model to expose information for.
   * @param modelVersions the versions of the model.
   */
  public ModelInfo(Model model, ModelVersionInfo[] modelVersions) {
    this.name = model.name();
    this.properties = model.properties();
    this.comment = model.comment();
    this.audit = model.auditInfo();
    this.lastVersion = model.latestVersion();
    this.versions = modelVersions;
  }

  public ModelInfo(
      String name, Map<String, String> properties, String comment, ModelVersionInfo[] versions) {
    this.name = name;
    this.properties = properties == null ? Collections.emptyMap() : properties;
    this.comment = comment == null ? "" : comment;
    this.audit = null;
    this.lastVersion = 0;
    this.versions = versions;
  }

  /**
   * Returns the comment of the model.
   *
   * @return the comment of the model or null if not set.
   */
  @Nullable
  public String getComment() {
    return comment;
  }

  /**
   * Returns the audit information of the model.
   *
   * @return the audit information of the model or null if not set.
   */
  @Nullable
  public Audit getAudit() {
    return audit;
  }

  /**
   * Returns the versions of the model.
   *
   * @return the versions of the model or null if not set.
   */
  @Nullable
  public ModelVersionInfo[] modelVersions() {
    return versions;
  }
}
