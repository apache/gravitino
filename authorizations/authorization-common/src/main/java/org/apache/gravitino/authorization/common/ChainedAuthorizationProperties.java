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
package org.apache.gravitino.authorization.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The properties for Chained authorization plugin. <br>
 * <br>
 * Configuration Example: <br>
 * "authorization.chain.plugins" = "hive1,hdfs1" <br>
 * "authorization.chain.hive1.provider" = "ranger"; <br>
 * "authorization.chain.hive1.ranger.service.type" = "HadoopSQL"; <br>
 * "authorization.chain.hive1.ranger.service.name" = "hiveDev"; <br>
 * "authorization.chain.hive1.ranger.auth.type" = "simple"; <br>
 * "authorization.chain.hive1.ranger.admin.url" = "http://localhost:6080"; <br>
 * "authorization.chain.hive1.ranger.username" = "admin"; <br>
 * "authorization.chain.hive1.ranger.password" = "admin"; <br>
 * "authorization.chain.hdfs1.provider" = "ranger"; <br>
 * "authorization.chain.hdfs1.ranger.service.type" = "HDFS"; <br>
 * "authorization.chain.hdfs1.ranger.service.name" = "hdfsDev"; <br>
 * "authorization.chain.hdfs1.ranger.auth.type" = "simple"; <br>
 * "authorization.chain.hdfs1.ranger.admin.url" = "http://localhost:6080"; <br>
 * "authorization.chain.hdfs1.ranger.username" = "admin"; <br>
 * "authorization.chain.hdfs1.ranger.password" = "admin"; <br>
 */
public class ChainedAuthorizationProperties extends AuthorizationProperties {
  private static final String PLUGINS_SPLITTER = ",";
  /** Chained authorization plugin names */
  public static final String CHAIN_PLUGINS_PROPERTIES_KEY = "authorization.chain.plugins";

  /** Chained authorization plugin provider */
  public static final String CHAIN_PROVIDER = "authorization.chain.*.provider";

  public ChainedAuthorizationProperties(Map<String, String> properties) {
    super(properties);
  }

  @Override
  public String getPropertiesPrefix() {
    return "authorization.chain";
  }

  public String getPluginProvider(String pluginName) {
    return properties.get(getPropertiesPrefix() + "." + pluginName + ".provider");
  }

  public List<String> plugins() {
    return Arrays.asList(properties.get(CHAIN_PLUGINS_PROPERTIES_KEY).split(PLUGINS_SPLITTER));
  }

  public Map<String, String> fetchAuthPluginProperties(String pluginName) {
    Preconditions.checkArgument(
        properties.containsKey(CHAIN_PLUGINS_PROPERTIES_KEY)
            && properties.get(CHAIN_PLUGINS_PROPERTIES_KEY) != null,
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, CHAIN_PLUGINS_PROPERTIES_KEY));

    String[] pluginNames = properties.get(CHAIN_PLUGINS_PROPERTIES_KEY).split(PLUGINS_SPLITTER);
    Preconditions.checkArgument(
        Arrays.asList(pluginNames).contains(pluginName),
        String.format("pluginName %s must be one of %s", pluginName, Arrays.toString(pluginNames)));

    String regex = "^authorization\\.chain\\.(" + pluginName + ")\\..*";
    Pattern pattern = Pattern.compile(regex);

    Map<String, String> filteredProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Matcher matcher = pattern.matcher(entry.getKey());
      if (matcher.matches()) {
        filteredProperties.put(entry.getKey(), entry.getValue());
      }
    }

    String removeRegex = "^authorization\\.chain\\.(" + pluginName + ")\\.";
    Pattern removePattern = Pattern.compile(removeRegex);

    Map<String, String> resultProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : filteredProperties.entrySet()) {
      Matcher removeMatcher = removePattern.matcher(entry.getKey());
      if (removeMatcher.find()) {
        resultProperties.put(removeMatcher.replaceFirst("authorization."), entry.getValue());
      }
    }

    return resultProperties;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        properties.containsKey(CHAIN_PLUGINS_PROPERTIES_KEY),
        String.format(ErrorMessages.MISSING_REQUIRED_ARGUMENT, CHAIN_PLUGINS_PROPERTIES_KEY));
    List<String> pluginNames =
        Arrays.stream(properties.get(CHAIN_PLUGINS_PROPERTIES_KEY).split(PLUGINS_SPLITTER))
            .map(String::trim)
            .filter(v -> !v.isEmpty())
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        !pluginNames.isEmpty(),
        String.format("%s must have at least one plugin name", CHAIN_PLUGINS_PROPERTIES_KEY));
    Preconditions.checkArgument(
        pluginNames.size() == pluginNames.stream().distinct().count(),
        "Duplicate plugin name in %s: %s",
        CHAIN_PLUGINS_PROPERTIES_KEY,
        pluginNames);
    pluginNames.stream()
        .filter(v -> v.contains("."))
        .forEach(
            v -> {
              throw new IllegalArgumentException(
                  String.format(
                      "Plugin name cannot be contain `.` character in the `%s = %s`.",
                      CHAIN_PLUGINS_PROPERTIES_KEY, properties.get(CHAIN_PLUGINS_PROPERTIES_KEY)));
            });

    Pattern pattern = Pattern.compile("^authorization\\.chain\\..*\\..*$");
    Map<String, String> filteredProperties =
        properties.entrySet().stream()
            .filter(entry -> pattern.matcher(entry.getKey()).matches())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    String pluginNamesPattern = String.join("|", pluginNames);
    Pattern patternPluginNames =
        Pattern.compile("^authorization\\.chain\\.(" + pluginNamesPattern + ")\\..*$");
    for (String key : filteredProperties.keySet()) {
      Matcher matcher = patternPluginNames.matcher(key);
      Preconditions.checkArgument(
          matcher.matches(),
          "The key %s does not match the pattern %s",
          key,
          patternPluginNames.pattern());
    }

    // Generate regex patterns from wildcardProperties
    List<String> wildcardProperties = ImmutableList.of(CHAIN_PROVIDER);
    for (String pluginName : pluginNames) {
      List<Pattern> patterns =
          wildcardProperties.stream()
              .map(wildcard -> "^" + wildcard.replace("*", pluginName) + "$")
              .map(Pattern::compile)
              .collect(Collectors.toList());
      // Validate properties keys
      for (Pattern pattern1 : patterns) {
        boolean matches =
            filteredProperties.keySet().stream().anyMatch(key -> pattern1.matcher(key).matches());
        Preconditions.checkArgument(
            matches,
            "Missing required properties %s for plugin: %s",
            filteredProperties,
            pattern1.pattern());
      }
    }
  }
}
