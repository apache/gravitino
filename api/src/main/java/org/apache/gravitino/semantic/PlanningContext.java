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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/**
 * The context supplied to a {@link SemanticQueryPlanner} for one planning request.
 *
 * <p>The context identifies the Semantic Model being planned and carries planner-specific
 * properties. It does not contain caller credentials; authorization has already been performed by
 * the server.
 */
@Evolving
public final class PlanningContext {

  private final NameIdentifier semanticModel;
  private final Map<String, String> properties;

  private PlanningContext(NameIdentifier semanticModel, Map<String, String> properties) {
    Preconditions.checkArgument(semanticModel != null, "semanticModel must not be null");
    Preconditions.checkArgument(
        semanticModel.name() != null && !semanticModel.name().isEmpty(),
        "semanticModel name must not be null or empty");
    Preconditions.checkArgument(properties != null, "properties must not be null");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Preconditions.checkArgument(
          entry.getKey() != null && !entry.getKey().isEmpty(),
          "property names must not be null or empty");
      Preconditions.checkArgument(
          entry.getValue() != null, "property %s must not have a null value", entry.getKey());
    }
    this.semanticModel = semanticModel;
    this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  /**
   * Creates a context without planner-specific properties.
   *
   * @param semanticModel The complete Semantic Model identifier.
   * @return The planning context.
   * @throws IllegalArgumentException If the identifier is invalid.
   */
  public static PlanningContext of(NameIdentifier semanticModel) {
    return new PlanningContext(semanticModel, Collections.emptyMap());
  }

  /**
   * Creates a context with planner-specific properties.
   *
   * @param semanticModel The complete Semantic Model identifier.
   * @param properties The planner-specific properties; must not be null or contain null values.
   * @return The planning context.
   * @throws IllegalArgumentException If the identifier or properties are invalid.
   */
  public static PlanningContext of(NameIdentifier semanticModel, Map<String, String> properties) {
    return new PlanningContext(semanticModel, properties);
  }

  /**
   * Returns the complete Semantic Model identifier being planned.
   *
   * @return The Semantic Model identifier.
   */
  public NameIdentifier semanticModel() {
    return semanticModel;
  }

  /**
   * Returns the planner-specific properties.
   *
   * @return An unmodifiable map of properties; empty if there are none.
   */
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanningContext)) {
      return false;
    }
    PlanningContext that = (PlanningContext) other;
    return semanticModel.equals(that.semanticModel) && properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(semanticModel, properties);
  }

  @Override
  public String toString() {
    return "PlanningContext{"
        + "semanticModel="
        + semanticModel
        + ", properties="
        + properties
        + '}';
  }
}
