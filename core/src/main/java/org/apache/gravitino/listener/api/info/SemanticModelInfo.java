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
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/**
 * SemanticModelInfo exposes Semantic Model information for event listeners; it is read-only. The
 * definition is already an immutable value, so it is referenced directly.
 */
@DeveloperApi
public final class SemanticModelInfo {
  private final String name;
  @Nullable private final String comment;
  private final SemanticModelDefinition definition;
  private final Map<String, String> properties;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs a SemanticModelInfo from a {@link SemanticModel}.
   *
   * @param semanticModel The Semantic Model to expose.
   */
  public SemanticModelInfo(SemanticModel semanticModel) {
    this(
        semanticModel.name(),
        semanticModel.comment(),
        semanticModel.definition(),
        semanticModel.properties(),
        semanticModel.auditInfo());
  }

  /**
   * Constructs a SemanticModelInfo with the given fields.
   *
   * @param name Semantic Model name.
   * @param comment Optional comment.
   * @param definition The immutable Semantic Model definition.
   * @param properties Gravitino-specific properties; copied defensively.
   * @param auditInfo Optional audit information.
   */
  public SemanticModelInfo(
      String name,
      @Nullable String comment,
      SemanticModelDefinition definition,
      @Nullable Map<String, String> properties,
      @Nullable Audit auditInfo) {
    this.name = name;
    this.comment = comment;
    this.definition = definition;
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.auditInfo = auditInfo;
  }

  /**
   * Returns the Semantic Model name.
   *
   * @return The Semantic Model name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the optional comment for the Semantic Model.
   *
   * @return The comment, or {@code null} if it is not set.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the Semantic Model definition.
   *
   * @return The Semantic Model definition.
   */
  public SemanticModelDefinition definition() {
    return definition;
  }

  /**
   * Returns the Gravitino-specific properties of the Semantic Model.
   *
   * @return The immutable Semantic Model properties.
   */
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the optional audit information for the Semantic Model.
   *
   * @return The audit information, or {@code null} if it is not set.
   */
  @Nullable
  public Audit auditInfo() {
    return auditInfo;
  }
}
