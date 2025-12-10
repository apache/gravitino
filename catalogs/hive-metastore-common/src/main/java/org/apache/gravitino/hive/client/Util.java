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
package org.apache.gravitino.hive.client;

import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Util {

  public static final String HIVE_CONFIG_RESOURCES = "hive.config.resources";

  public static Configuration buildConfigurationFromProperties(
      Properties properties, Configuration config) {
    try {
      String configResources = properties.getProperty(HIVE_CONFIG_RESOURCES);
      if (StringUtils.isNotBlank(configResources)) {
        for (String resource : configResources.split(",")) {
          resource = resource.trim();
          if (StringUtils.isNotBlank(resource)) {
            config.addResource(new Path(resource));
          }
        }
      }

      properties.forEach((k, v) -> config.set(k.toString(), v.toString()));
      return config;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create configuration", e);
    }
  }
}
