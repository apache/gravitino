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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.AuthorizationPropertiesMetadata;

/**
 * WildcardPropertiesMeta is a interface class to support wildcard in the properties metadata. <br>
 * <br>
 * WildcardPropertiesMetadata interface defines: <br>
 * Prefix.Wildcard = "" <br>
 * Prefix.*.properties-key1 = "" <br>
 * Prefix.*.properties-key2 = "" <br>
 * NOTE: Prefix support multiple segment, separated by dot. <br>
 * NOTE: properties-key{N} support multiple segment, separated by dot. <br>
 * <br>
 * Use define a WildcardPropertiesMetadata object: <br>
 * a1.b1.c1.WildcardNode = "WildcardValue1,WildcardValue2" <br>
 * a1.b1.c1.{WildcardValue1}.x1.y1.z1 = "WildcardValue1 properties-key1 value" <br>
 * a1.b1.c1.{WildcardValue1}.x1.y2.z2 = "WildcardValue1 properties-key2 value" <br>
 * a1.b1.c1.{WildcardValue1}.x1.y2.z3 = "WildcardValue1 properties-key3 value" <br>
 * a1.b1.c1.{WildcardValue2}.x1.y1.z1 = "WildcardValue2 properties-key1 value" <br>
 * a1.b1.c1.{WildcardValue2}.x1.y2.z2 = "WildcardValue2 properties-key2 value" <br>
 * a1.b1.c1.{WildcardValue2}.x1.y2.z3 = "WildcardValue2 properties-key3 value" <br>
 * <br>
 * Configuration Example: {@link AuthorizationPropertiesMetadata} <br>
 * "authorization.chain.plugins" = "hive1,hdfs1" <br>
 * "authorization.chain.hive1.provider" = "ranger"; <br>
 * "authorization.chain.hive1.catalog-provider" = "hive"; <br>
 * "authorization.chain.hive1.ranger.auth.type" = "simple"; <br>
 * "authorization.chain.hive1.ranger.admin.url" = "http://localhost:6080"; <br>
 * "authorization.chain.hive1.ranger.username" = "admin"; <br>
 * "authorization.chain.hive1.ranger.password" = "admin"; <br>
 * "authorization.chain.hive1.ranger.service.name" = "hiveDev"; <br>
 * "authorization.chain.hdfs1.provider" = "ranger"; <br>
 * "authorization.chain.hdfs1.catalog-provider" = "hadoop"; <br>
 * "authorization.chain.hdfs1.ranger.auth.type" = "simple"; <br>
 * "authorization.chain.hdfs1.ranger.admin.url" = "http://localhost:6080"; <br>
 * "authorization.chain.hdfs1.ranger.username" = "admin"; <br>
 * "authorization.chain.hdfs1.ranger.password" = "admin"; <br>
 * "authorization.chain.hdfs1.ranger.service.name" = "hdfsDev"; <br>
 */
public interface WildcardPropertiesMetadata {
  class Constants {
    public static final String WILDCARD = "*";
    public static final String WILDCARD_CONFIG_VALUES_SPLITTER = ",";
  }

  /** The prefix name */
  String prefixName();
  /** The WildcardNode define name */
  String wildcardName();
  /** The `FirstNode.SecondNode.WildcardNode` properties key name */
  default String wildcardNodePropertyKey() {
    return String.format("%s.%s", prefixName(), wildcardName());
  }
  /** Get the property value by wildcard value and property key */
  default String getPropertyValue(String wildcardValue, String propertyKey) {
    return String.format("%s.%s.%s", prefixName(), wildcardValue, propertyKey);
  }

  /**
   * Validate the wildcard properties in the properties metadata.
   *
   * @param propertiesMetadata the properties metadata
   * @param properties the properties
   * @throws IllegalArgumentException if the wildcard properties are not valid
   */
  static void validate(PropertiesMetadata propertiesMetadata, Map<String, String> properties)
      throws IllegalArgumentException {
    // Get all wildcard properties from PropertiesMetadata
    List<String> wildcardProperties =
        propertiesMetadata.propertyEntries().keySet().stream()
            .filter(propertiesMetadata::isWildcardProperty)
            .collect(Collectors.toList());
    if (wildcardProperties.size() > 0) {
      // Find the wildcard config key from the properties
      List<String> wildcardNodePropertyKeys =
          wildcardProperties.stream()
              .filter(key -> !key.contains(WildcardPropertiesMetadata.Constants.WILDCARD))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          wildcardNodePropertyKeys.size() == 1,
          "Only one wildcard config key is allowed, found: %s",
          wildcardNodePropertyKeys);
      String wildcardNodePropertyKey = wildcardNodePropertyKeys.get(0);
      String wildcardValue = properties.get(wildcardNodePropertyKey);
      if (wildcardValue == null || wildcardValue.isEmpty()) {
        return;
      }

      // Get the wildcard values from the properties
      List<String> wildcardValues =
          Arrays.stream(wildcardValue.split(Constants.WILDCARD_CONFIG_VALUES_SPLITTER))
              .map(String::trim)
              .collect(Collectors.toList());
      wildcardValues.stream()
          .filter(v -> v.contains("."))
          .forEach(
              v -> {
                throw new IllegalArgumentException(
                    String.format(
                        "Wildcard property values cannot be set with `.` character in the `%s = %s`.",
                        wildcardNodePropertyKey, properties.get(wildcardNodePropertyKey)));
              });
      Preconditions.checkArgument(
          wildcardValues.size() == wildcardValues.stream().distinct().count(),
          "Duplicate values in wildcard config values: %s",
          wildcardValues);

      // Get all wildcard properties with wildcard values
      List<Pattern> patterns =
          wildcardProperties.stream()
              .filter(k -> k.contains(Constants.WILDCARD))
              .collect(Collectors.toList())
              .stream()
              .map(wildcard -> wildcard.replace(".", "\\.").replace(Constants.WILDCARD, "([^.]+)"))
              .map(Pattern::compile)
              .collect(Collectors.toList());

      String secondNodePropertyKey = ((WildcardPropertiesMetadata) propertiesMetadata).prefixName();
      for (String key :
          properties.keySet().stream()
              .filter(
                  k -> !k.equals(wildcardNodePropertyKey) && k.startsWith(secondNodePropertyKey))
              .collect(Collectors.toList())) {
        boolean matches =
            patterns.stream()
                .anyMatch(
                    pattern -> {
                      Matcher matcher = pattern.matcher(key);
                      if (matcher.find()) {
                        String group = matcher.group(1);
                        return wildcardValues.contains(group);
                      } else {
                        return false;
                      }
                    });
        Preconditions.checkArgument(
            matches,
            "Wildcard properties `%s` not a valid wildcard config with values: %s",
            key,
            wildcardValues);
      }
    }
  }
}
