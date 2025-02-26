package org.apache.gravitino.flink.connector.jdbc;

import org.apache.gravitino.flink.connector.PropertiesConverter;

public class JdbcPropertiesConverter implements PropertiesConverter {

  public static final JdbcPropertiesConverter INSTANCE = new JdbcPropertiesConverter();

  private JdbcPropertiesConverter() {}

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return JdbcPropertiesConstants.flinkToGravitinoMap.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return JdbcPropertiesConstants.gravitinoToFlinkMap.get(configKey);
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoJdbcCatalogFactoryOptions.IDENTIFIER;
  }
}
