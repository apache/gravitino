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

import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.Audit;
import org.apache.gravitino.model.ModelVersion;

/**
 * {@link ModelVersionInfo} exposes model version information for event listener, it's supposed to
 * be read only. Most of the fields are shallow copied internally not deep copies for performance.
 */
public class ModelVersionInfo {
  @Getter private final String uri;
  @Getter @Nullable private final String[] aliases;
  @Nullable private final String comment;
  @Getter private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs model version information based on a given {@link ModelVersion}.
   *
   * @param modelVersion the model version to expose information for.
   */
  public ModelVersionInfo(ModelVersion modelVersion) {
    this.uri = modelVersion.uri();
    this.aliases = modelVersion.aliases();
    this.comment = modelVersion.comment();
    this.properties = modelVersion.properties();
    this.auditInfo = modelVersion.auditInfo();
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
      String uri, String[] aliases, String comment, Map<String, String> properties) {
    this.uri = uri;
    this.aliases = aliases;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = null;
  }

  /**
   * Returns the comment of the model version.
   *
   * @return the comment of the model version or null if not set.
   */
  @Nullable
  public String getComment() {
    return comment;
  }

  /**
   * Returns the audit information of the model version.
   *
   * @return the audit information of the model version or null if not set.
   */
  @Nullable
  public Audit getAudit() {
    return auditInfo;
  }
}
