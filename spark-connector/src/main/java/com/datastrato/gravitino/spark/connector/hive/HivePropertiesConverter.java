/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.hive;

import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import java.util.HashMap;
import java.util.Map;

/** Transform hive catalog properties between Spark and Gravitino. Will implement in another PR. */
public class HivePropertiesConverter implements PropertiesConverter {

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
