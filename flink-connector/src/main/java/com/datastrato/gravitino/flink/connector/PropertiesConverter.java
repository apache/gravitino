/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector;

import java.util.Map;
import org.apache.flink.configuration.Configuration;

/**
 * PropertiesConverter is used to convert properties between Flink properties and Gravitino
 * properties
 */
public interface PropertiesConverter {

  String FLINK_PROPERTY_PREFIX = "flink.bypass.";

  /**
   * Converts properties from application provided properties and Flink connector properties to
   * Gravitino properties.
   *
   * @param flinkConf The configuration provided by Flink.
   * @return properties for the Gravitino connector.
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
}
