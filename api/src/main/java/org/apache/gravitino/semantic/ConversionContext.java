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
import org.apache.gravitino.annotation.Evolving;

/**
 * Engine-side configuration supplied to a {@link SemanticQueryPlanConverter}.
 *
 * <p>The context carries properties that the plan must not know about, such as a
 * Gravitino-catalog-to-engine-catalog mapping for federated sources.
 */
@Evolving
public final class ConversionContext {

  private final Map<String, String> properties;

  private ConversionContext(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "properties must not be null");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Preconditions.checkArgument(
          entry.getKey() != null && !entry.getKey().isEmpty(),
          "property names must not be null or empty");
      Preconditions.checkArgument(
          entry.getValue() != null, "property %s must not have a null value", entry.getKey());
    }
    this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  /**
   * Creates a conversion context.
   *
   * @param properties Engine-side properties; must not be null or contain null values.
   * @return The conversion context.
   */
  public static ConversionContext of(Map<String, String> properties) {
    return new ConversionContext(properties);
  }

  /**
   * Returns the engine-side properties.
   *
   * @return An unmodifiable map of properties; empty if there are none.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
