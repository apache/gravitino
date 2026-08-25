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
package org.apache.gravitino.client;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** An immutable client-side Semantic Model returned by the Gravitino REST API. */
class GenericSemanticModel implements SemanticModel {

  private final String name;
  private final SemanticModelDefinition definition;
  private final Map<String, String> properties;
  private final Audit audit;

  @Nullable private final String comment;

  GenericSemanticModel(SemanticModel semanticModel) {
    this.name = semanticModel.name();
    this.comment = semanticModel.comment();
    this.definition = semanticModel.definition();
    Map<String, String> sourceProperties = semanticModel.properties();
    this.properties =
        sourceProperties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(sourceProperties));
    this.audit = semanticModel.auditInfo();
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /** {@inheritDoc} */
  @Override
  public SemanticModelDefinition definition() {
    return definition;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** {@inheritDoc} */
  @Override
  public Audit auditInfo() {
    return audit;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof GenericSemanticModel)) {
      return false;
    }
    GenericSemanticModel that = (GenericSemanticModel) other;
    return name.equals(that.name)
        && Objects.equals(comment, that.comment)
        && definition.equals(that.definition)
        && properties.equals(that.properties)
        && audit.equals(that.audit);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(name, comment, definition, properties, audit);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "GenericSemanticModel{"
        + "name='"
        + name
        + '\''
        + ", comment='"
        + comment
        + '\''
        + ", definition="
        + definition
        + ", properties="
        + properties
        + ", audit="
        + audit
        + '}';
  }
}
