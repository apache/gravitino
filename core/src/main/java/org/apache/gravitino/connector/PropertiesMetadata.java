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

import java.util.Comparator;
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
    // First check non-prefix exact match
    PropertyEntry<?> directEntry = propertyEntries().get(propertyName);
    if (directEntry != null && !directEntry.isPrefix() && directEntry.isReserved()) {
      return true;
    }

    // Then check prefix properties using stream
    return propertyEntries().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getValue().isPrefix()
                    && propertyName.startsWith(entry.getKey())
                    && entry.getValue().isReserved());
  }

  /**
   * Check if the property is required.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and required, false otherwise.
   */
  default boolean isRequiredProperty(String propertyName) {
    // First check non-prefix exact match
    PropertyEntry<?> directEntry = propertyEntries().get(propertyName);
    if (directEntry != null && !directEntry.isPrefix() && directEntry.isRequired()) {
      return true;
    }

    // Then check prefix properties using stream
    return propertyEntries().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getValue().isPrefix()
                    && propertyName.startsWith(entry.getKey())
                    && entry.getValue().isRequired());
  }

  /**
   * Check if the property is immutable.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and immutable, false otherwise.
   */
  default boolean isImmutableProperty(String propertyName) {
    // First check non-prefix exact match
    PropertyEntry<?> directEntry = propertyEntries().get(propertyName);
    if (directEntry != null && !directEntry.isPrefix() && directEntry.isImmutable()) {
      return true;
    }

    // Then check prefix properties using stream
    return propertyEntries().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getValue().isPrefix()
                    && propertyName.startsWith(entry.getKey())
                    && entry.getValue().isImmutable());
  }

  /**
   * Check if the property is hidden.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and hidden, false otherwise.
   */
  default boolean isHiddenProperty(String propertyName) {
    // First check non-prefix exact match
    PropertyEntry<?> directEntry = propertyEntries().get(propertyName);
    if (directEntry != null && !directEntry.isPrefix() && directEntry.isHidden()) {
      return true;
    }

    // Then check prefix properties using stream
    return propertyEntries().entrySet().stream()
        .anyMatch(
            entry ->
                entry.getValue().isPrefix()
                    && propertyName.startsWith(entry.getKey())
                    && entry.getValue().isHidden());
  }

  /** @return true if the property is existed, false otherwise. */
  default boolean containsProperty(String propertyName) {
    return (propertyEntries().containsKey(propertyName)
            && !propertyEntries().get(propertyName).isPrefix())
        || propertyEntries().entrySet().stream()
            .anyMatch(
                entry -> entry.getValue().isPrefix() && propertyName.startsWith(entry.getKey()));
  }

  /**
   * Get the value of the property from the given properties map.
   *
   * @param properties The properties map.
   * @param propertyName The name of the property.
   * @return the value object of the property.
   */
  default Object getOrDefault(Map<String, String> properties, String propertyName) {
    PropertyEntry<?> propertyEntry = getPropertyEntry(propertyName);

    if (properties != null && properties.containsKey(propertyName)) {
      return propertyEntry.decode(properties.get(propertyName));
    }
    return propertyEntry.getDefaultValue();
  }

  /**
   * Get the default value of the property.
   *
   * @param propertyName The name of the property.
   * @return the default value object of the property.
   */
  default Object getDefaultValue(String propertyName) {
    PropertyEntry<?> propertyEntry = getPropertyEntry(propertyName);

    return propertyEntry.getDefaultValue();
  }

  /**
   * Get the property entry of the property.
   *
   * @param propertyName The name of the property.
   * @return the property entry object of the property.
   * @throws IllegalArgumentException if the property is not defined.
   */
  default PropertyEntry<?> getPropertyEntry(String propertyName) throws IllegalArgumentException {
    if (!containsProperty(propertyName)) {
      throw new IllegalArgumentException("Property is not defined: " + propertyName);
    }

    return propertyEntries().get(propertyName) != null
        ? propertyEntries().get(propertyName)
        : propertyEntries().entrySet().stream()
            .filter(e -> e.getValue().isPrefix() && propertyName.startsWith(e.getKey()))
            // get the longest prefix property
            .max(Map.Entry.comparingByKey(Comparator.comparingInt(String::length)))
            .map(Map.Entry::getValue)
            // should not happen since containsProperty check is done before
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No matching prefix property found for: " + propertyName));
  }
}
