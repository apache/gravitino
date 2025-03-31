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

import com.google.common.base.Preconditions;
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

    // Then check property prefixes
    try {
      return getPropertyPrefixEntry(propertyName).isReserved();
    } catch (IllegalArgumentException e) {
      return false;
    }
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

    // Then check property prefixes
    try {
      return getPropertyPrefixEntry(propertyName).isRequired();
    } catch (IllegalArgumentException e) {
      return false;
    }
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

    // Then check property prefixes
    try {
      return getPropertyPrefixEntry(propertyName).isImmutable();
    } catch (IllegalArgumentException e) {
      return false;
    }
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

    // Then check property prefixes
    try {
      return getPropertyPrefixEntry(propertyName).isHidden();
    } catch (IllegalArgumentException e) {
      return false;
    }
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
   * Get the property entry of the property. The non-prefix property entry will be returned if
   * exists, otherwise the longest prefix property entry will be returned.
   *
   * <p>For example, if there are two property prefix entries "foo." and "foo.bar", and the property
   * name is "foo.bar.baz", the entry for "foo.bar" will be returned. If the property entry
   * "foo.bar.baz" is defined, it will be returned instead.
   *
   * @param propertyName The name of the property.
   * @return the property entry object of the property.
   * @throws IllegalArgumentException if the property is not defined.
   */
  default PropertyEntry<?> getPropertyEntry(String propertyName) throws IllegalArgumentException {
    if (!containsProperty(propertyName)) {
      throw new IllegalArgumentException("Property is not defined: " + propertyName);
    }

    try {
      return getNonPrefixEntry(propertyName);
    } catch (IllegalArgumentException e) {
      return getPropertyPrefixEntry(propertyName);
    }
  }

  /**
   * Get the property prefix entry of the property. If there are multiple property prefix entries
   * matching the property name, the longest prefix entry will be returned.
   *
   * <p>For example, if there are two property prefix entries "foo." and "foo.bar", and the property
   * name is "foo.bar.baz", the entry for "foo.bar" will be returned.
   *
   * @param propertyName The name of the property.
   * @return the property prefix entry object of the property.
   * @throws IllegalArgumentException if the property prefix is not defined.
   */
  default PropertyEntry<?> getPropertyPrefixEntry(String propertyName)
      throws IllegalArgumentException {
    return propertyEntries().entrySet().stream()
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

  /**
   * Get the non-prefix property entry of the property. That is, the property entry that is not a
   * prefix.
   *
   * @param propertyName The name of the property.
   * @return the non-prefix property entry object of the property.
   * @throws IllegalArgumentException if the property is not defined or is a prefix.
   */
  default PropertyEntry<?> getNonPrefixEntry(String propertyName) throws IllegalArgumentException {
    Preconditions.checkArgument(
        propertyEntries().containsKey(propertyName)
            && !propertyEntries().get(propertyName).isPrefix(),
        "Property is not defined: " + propertyName);
    return propertyEntries().get(propertyName);
  }
}
