package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.paimon.options.CatalogOptions;

public class PaimonPropertiesConverter implements PropertiesConverter {

  public static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();

  private PaimonPropertiesConverter() {}

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties = Maps.newHashMap();
    flinkCatalogProperties.putAll(gravitinoProperties);
    String backendType =
        flinkCatalogProperties.get(GravitinoPaimonCatalogFactoryOptions.backendType.key());
    if (PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(backendType)) {
      flinkCatalogProperties.put(CatalogOptions.METASTORE.key(), backendType);
      flinkCatalogProperties.put(
          CatalogOptions.URI.key(), gravitinoProperties.get(PaimonConfig.CATALOG_URI.getKey()));
      flinkCatalogProperties.put(
          "jdbc.user", gravitinoProperties.get(PaimonConfig.CATALOG_JDBC_USER.getKey()));
      flinkCatalogProperties.put(
          "jdbc.password", gravitinoProperties.get(PaimonConfig.CATALOG_JDBC_PASSWORD.getKey()));
    } else if (PaimonCatalogBackend.HIVE.name().equalsIgnoreCase(backendType)) {
      throw new UnsupportedOperationException(
          "The Gravitino Connector does not currently support creating a Paimon Catalog that uses Hive Metastore.");
    }
    flinkCatalogProperties.put(
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoPaimonCatalogFactoryOptions.IDENTIFIER);
    return flinkCatalogProperties;
  }
}
