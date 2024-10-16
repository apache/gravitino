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
package org.apache.gravitino.connector;

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;

/** The PropertiesMetadata class is responsible for managing property metadata. */
@Evolving
public interface PropertiesMetadata {

  /** @return the defined property entries for the entity. */
  Map<String, PropertyEntry<?>> propertyEntries();

  /**
   * Check if the property is reserved.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and reserved, false otherwise.
   */
  default boolean isReservedProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isReserved();
  }

  /**
   * Check if the property is required.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and required, false otherwise.
   */
  default boolean isRequiredProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isRequired();
  }

  /**
   * Check if the property is immutable.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and immutable, false otherwise.
   */
  default boolean isImmutableProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isImmutable();
  }

  /**
   * Check if the property is hidden.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and hidden, false otherwise.
   */
  default boolean isHiddenProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName)
        && propertyEntries().get(propertyName).isHidden();
  }

  /** @return true if the property is existed, false otherwise. */
  default boolean containsProperty(String propertyName) {
    return propertyEntries().containsKey(propertyName);
  }

  /**
   * Get the value of the property from the given properties map.
   *
   * @param properties The properties map.
   * @param propertyName The name of the property.
   * @return the value object of the property.
   */
  default Object getOrDefault(Map<String, String> properties, String propertyName) {
    if (!containsProperty(propertyName)) {
      throw new IllegalArgumentException("Property is not defined: " + propertyName);
    }

    if (properties != null && properties.containsKey(propertyName)) {
      return propertyEntries().get(propertyName).decode(properties.get(propertyName));
    }
    return propertyEntries().get(propertyName).getDefaultValue();
  }

  /**
   * Get the default value of the property.
   *
   * @param propertyName The name of the property.
   * @return the default value object of the property.
   */
  default Object getDefaultValue(String propertyName) {
    if (!containsProperty(propertyName)) {
      throw new IllegalArgumentException("Property is not defined: " + propertyName);
    }

    return propertyEntries().get(propertyName).getDefaultValue();
  }
}
