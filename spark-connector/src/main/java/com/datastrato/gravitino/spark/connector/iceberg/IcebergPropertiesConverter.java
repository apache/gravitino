package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import java.util.HashMap;
import java.util.Map;

/** Transform iceberg catalog properties between Spark and Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {
  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
