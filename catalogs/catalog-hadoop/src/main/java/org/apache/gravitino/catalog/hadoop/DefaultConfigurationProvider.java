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
package org.apache.gravitino.catalog.hadoop;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;

public class DefaultConfigurationProvider implements ConfigurationProvider {
  @Override
  public Configuration getConfiguration(Map<String, String> conf) {
    Configuration configuration = new Configuration();

    // Get all configurations that start with the 'gravitino.bypass' prefix and remove the prefix
    // to set them in the configuration.
    Map<String, String> bypassConfigs =
        conf.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_BYPASS_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CATALOG_BYPASS_PREFIX.length()),
                    Map.Entry::getValue));
    bypassConfigs.forEach(configuration::set);
    return configuration;
  }
}
