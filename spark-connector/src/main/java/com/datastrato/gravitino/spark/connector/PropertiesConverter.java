/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import java.util.Map;

/** Transform table properties between Gravitino and Spark. */
public interface PropertiesConverter {
  Map<String, String> toGravitinoTableProperties(Map<String, String> properties);

  Map<String, String> toSparkTableProperties(Map<String, String> properties);
}
