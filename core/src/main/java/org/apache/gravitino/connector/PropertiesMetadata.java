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
import java.util.Optional;
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
    if (getNonPrefixEntry(propertyName).map(PropertyEntry::isReserved).orElse(false)) {
      return true;
    }

    // Then check property prefixes
    return getPropertyPrefixEntry(propertyName).map(PropertyEntry::isReserved).orElse(false);
  }

  /**
   * Check if the property is required.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and required, false otherwise.
   */
  default boolean isRequiredProperty(String propertyName) {
    // First check non-prefix exact match
    if (getNonPrefixEntry(propertyName).map(PropertyEntry::isRequired).orElse(false)) {
      return true;
    }

    // Then check property prefixes
    return getPropertyPrefixEntry(propertyName).map(PropertyEntry::isRequired).orElse(false);
  }

  /**
   * Check if the property is immutable.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and immutable, false otherwise.
   */
  default boolean isImmutableProperty(String propertyName) {
    // First check non-prefix exact match
    if (getNonPrefixEntry(propertyName).map(PropertyEntry::isImmutable).orElse(false)) {
      return true;
    }

    // Then check property prefixes
    return getPropertyPrefixEntry(propertyName).map(PropertyEntry::isImmutable).orElse(false);
  }

  /**
   * Check if the property is hidden.
   *
   * @param propertyName The name of the property.
   * @return true if the property is existed and hidden, false otherwise.
   */
  default boolean isHiddenProperty(String propertyName) {
    // First check non-prefix exact match
    if (getNonPrefixEntry(propertyName).map(PropertyEntry::isHidden).orElse(false)) {
      return true;
    }

    // Then check property prefixes
    return getPropertyPrefixEntry(propertyName).map(PropertyEntry::isHidden).orElse(false);
  }

  /**
   * Checks whether the specified property exists.
   *
   * @param propertyName the name of the property to check for existence
   * @return true if the property exists, false otherwise
   */
  default boolean containsProperty(String propertyName) {
    return getNonPrefixEntry(propertyName).isPresent()
        || getPropertyPrefixEntry(propertyName).isPresent();
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
   * <p>For example, if there are two property prefix entries "foo." and "foo.bar.", and the
   * property name is "foo.bar.baz", the entry for "foo.bar." will be returned. If the property
   * entry "foo.bar.baz" is defined, it will be returned instead.
   *
   * @param propertyName The name of the property.
   * @return the property entry object of the property.
   * @throws IllegalArgumentException if the property is not defined.
   */
  default PropertyEntry<?> getPropertyEntry(String propertyName) throws IllegalArgumentException {
    if (!containsProperty(propertyName)) {
      throw new IllegalArgumentException("Property is not defined: " + propertyName);
    }

    Optional<PropertyEntry<?>> nonPrefixEntry = getNonPrefixEntry(propertyName);
    if (nonPrefixEntry.isPresent()) {
      return nonPrefixEntry.get();
    }

    Optional<PropertyEntry<?>> prefixEntry = getPropertyPrefixEntry(propertyName);
    if (prefixEntry.isPresent()) {
      return prefixEntry.get();
    }

    throw new IllegalArgumentException("Property is not defined: " + propertyName);
  }
  /**
   * Get the property prefix entry of the property. If there are multiple property prefix entries
   * matching the property name, the longest prefix entry will be returned.
   *
   * <p>For example, if there are two property prefix entries "foo." and "foo.bar.", and the
   * property name is "foo.bar.baz", the entry for "foo.bar." will be returned.
   *
   * @param propertyName The name of the property.
   * @return an Optional containing the property prefix entry if it exists, or empty Optional
   *     otherwise.
   */
  default Optional<PropertyEntry<?>> getPropertyPrefixEntry(String propertyName) {
    return propertyEntries().entrySet().stream()
        .filter(e -> e.getValue().isPrefix() && propertyName.startsWith(e.getKey()))
        // get the longest prefix property
        .max(Map.Entry.comparingByKey(Comparator.comparingInt(String::length)))
        .map(Map.Entry::getValue);
  }

  /**
   * Get the non-prefix property entry of the property. That is, the property entry that is not a
   * prefix.
   *
   * @param propertyName The name of the property.
   * @return an Optional containing the non-prefix property entry if it exists, or empty Optional
   *     otherwise.
   */
  default Optional<PropertyEntry<?>> getNonPrefixEntry(String propertyName) {
    if (propertyEntries().containsKey(propertyName)
        && !propertyEntries().get(propertyName).isPrefix()) {
      return Optional.of(propertyEntries().get(propertyName));
    }
    return Optional.empty();
  }
}
