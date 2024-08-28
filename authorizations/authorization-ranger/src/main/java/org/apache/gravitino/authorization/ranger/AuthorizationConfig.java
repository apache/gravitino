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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class AuthorizationConfig extends Config {
  public static final ConfigEntry<List<String>> AUTHORIZATION_OWNER_PRIVILEGES =
      new ConfigBuilder("gravitino.authorization.owner.privileges")
          .doc("The privileges that the owner of a security object has.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .toSequence()
          .checkValue(
              valueList ->
                  valueList != null && valueList.stream().allMatch(StringUtils::isNotBlank),
              ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(Lists.newArrayList());

  public static final ConfigEntry<List<String>> RANGER_POLICY_RESOURCE_DEFINES =
      new ConfigBuilder("gravitino.authorization.ranger.policy.resource.defines")
          .doc("The resource defines that are used in Ranger policies.")
          .version(ConfigConstants.VERSION_0_7_0)
          .stringConf()
          .toSequence()
          .checkValue(
              valueList ->
                  valueList != null && valueList.stream().allMatch(StringUtils::isNotBlank),
              ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(Lists.newArrayList());

  public static final String PRIVILEGE_MAPPING_PREFIX =
      "gravitino.authorization.privilege.mapping.";

  public static AuthorizationConfig loadConfig(String provideName) {
    AuthorizationConfig authorizationConfig = new AuthorizationConfig();
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;
    String propertyFilePath =
        String.format("authorization-defs/authorization-%s.properties", provideName);

    // Only load from resources in test environment
    if (testEnv) {
      URL resourceUrl = AuthorizationConfig.class.getResource(File.separator + propertyFilePath);
      try {
        if (resourceUrl != null) {
          Properties properties =
              authorizationConfig.loadPropertiesFromFile(new File(resourceUrl.getPath()));
          authorizationConfig.loadFromProperties(properties);
          return authorizationConfig;
        }
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to load authorization config from resource " + resourceUrl, e);
      }
    }

    // Load from configuration directory in production environment
    String confPath =
        String.join(
            File.separator,
            System.getenv("GRAVITINO_HOME"),
            "authorizations",
            provideName,
            "conf",
            propertyFilePath);
    try {
      Properties properties = authorizationConfig.loadPropertiesFromFile(new File(confPath));
      authorizationConfig.loadFromProperties(properties);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to load authorization config from file " + confPath, e);
    }
    return authorizationConfig;
  }
}
