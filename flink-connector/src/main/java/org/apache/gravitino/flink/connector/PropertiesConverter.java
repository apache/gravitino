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

package org.apache.gravitino.flink.connector;

import java.util.Map;
import org.apache.flink.configuration.Configuration;

/**
 * PropertiesConverter is used to convert properties between Flink properties and Apache Gravitino
 * properties
 */
public interface PropertiesConverter {

  String FLINK_PROPERTY_PREFIX = "flink.bypass.";

  /**
   * Converts properties from application provided properties and Flink connector properties to
   * Gravitino properties.
   *
   * @param flinkConf The configuration provided by Flink.
   * @return properties for the Gravitino catalog.
   */
  default Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    return flinkConf.toMap();
  }

  /**
   * Converts properties from Gravitino properties to Flink connector properties.
   *
   * @param gravitinoProperties The properties provided by Gravitino.
   * @return properties for the Flink connector.
   */
  default Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    return gravitinoProperties;
  }

  /**
   * Converts properties from Flink connector schema properties to Gravitino schema properties.
   *
   * @param flinkProperties The schema properties provided by Flink.
   * @return The schema properties for the Gravitino.
   */
  default Map<String, String> toGravitinoSchemaProperties(Map<String, String> flinkProperties) {
    return flinkProperties;
  }

  /**
   * Converts properties from Gravitino database properties to Flink connector schema properties.
   *
   * @param gravitinoProperties The schema properties provided by Gravitino.
   * @return The database properties for the Flink connector.
   */
  default Map<String, String> toFlinkDatabaseProperties(Map<String, String> gravitinoProperties) {
    return gravitinoProperties;
  }

  /**
   * Converts properties from Gravitino table properties to Flink connector table properties.
   *
   * @param gravitinoProperties The table properties provided by Gravitino.
   * @return The table properties for the Flink connector.
   */
  default Map<String, String> toFlinkTableProperties(Map<String, String> gravitinoProperties) {
    return gravitinoProperties;
  }

  /**
   * Converts properties from Flink connector table properties to Gravitino table properties.
   *
   * @param flinkProperties The table properties provided by Flink.
   * @return The table properties for the Gravitino.
   */
  default Map<String, String> toGravitinoTableProperties(Map<String, String> flinkProperties) {
    return flinkProperties;
  }
}
