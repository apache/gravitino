package org.apache.gravitino.flink.connector.jdbc;

import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.paimon.GravitinoPaimonCatalogFactoryOptions;

public class GravitinoJdbcCatalogFactory implements BaseCatalogFactory {

  @Override
  public String gravitinoCatalogProvider() {
    return GravitinoPaimonCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Catalog.Type gravitinoCatalogType() {
    return Catalog.Type.RELATIONAL;
  }

  @Override
  public PropertiesConverter propertiesConverter() {
    return JdbcPropertiesConverter.INSTANCE;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }
}
