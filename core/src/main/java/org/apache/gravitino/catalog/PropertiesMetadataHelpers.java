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
package org.apache.gravitino.catalog;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** This class contains helper methods for properties metadata. */
public class PropertiesMetadataHelpers {

  private PropertiesMetadataHelpers() {}

  public static <T> T checkValueFormat(String key, String value, Function<String, T> decoder) {
    try {
      return decoder.apply(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Invalid value: '%s' for property: '%s'", value, key), e);
    }
  }

  public static void validatePropertyForCreate(
      PropertiesMetadata propertiesMetadata, Map<String, String> properties)
      throws IllegalArgumentException {
    if (properties == null) {
      return;
    }

    List<String> reservedProperties =
        properties.keySet().stream()
            .filter(propertiesMetadata::isReservedProperty)
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        reservedProperties.isEmpty(),
        "Properties are reserved and cannot be set: %s",
        reservedProperties);

    List<String> absentProperties =
        propertiesMetadata.propertyEntries().keySet().stream()
            .filter(propertiesMetadata::isRequiredProperty)
            .filter(k -> !properties.containsKey(k))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        absentProperties.isEmpty(),
        "Properties are required and must be set: %s",
        absentProperties);

    // use decode function to validate the property values
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (propertiesMetadata.containsProperty(key)) {
        checkValueFormat(key, value, propertiesMetadata.propertyEntries().get(key)::decode);
      }
    }
  }

  public static void validatePropertyForAlter(
      PropertiesMetadata propertiesMetadata,
      Map<String, String> upserts,
      Map<String, String> deletes) {
    for (Map.Entry<String, String> entry : upserts.entrySet()) {
      PropertyEntry<?> propertyEntry = propertiesMetadata.propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be set");
        checkValueFormat(entry.getKey(), entry.getValue(), propertyEntry::decode);
      }
    }

    for (Map.Entry<String, String> entry : deletes.entrySet()) {
      PropertyEntry<?> propertyEntry = propertiesMetadata.propertyEntries().get(entry.getKey());
      if (Objects.nonNull(propertyEntry)) {
        Preconditions.checkArgument(
            !propertyEntry.isImmutable() && !propertyEntry.isReserved(),
            "Property " + propertyEntry.getName() + " is immutable or reserved, cannot be deleted");
      }
    }
  }
}
