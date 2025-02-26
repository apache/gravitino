package org.apache.gravitino.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactory;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;

public class GravitinoJdbcCatalog extends BaseCatalog {

  Catalog jdbcCatalog;

  protected GravitinoJdbcCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      PropertiesConverter propertiesConverter,
      PartitionConverter partitionConverter) {
    super(context.getName(), defaultDatabase, propertiesConverter, partitionConverter);
    JdbcCatalogFactory jdbcCatalogFactory = new JdbcCatalogFactory();
    this.jdbcCatalog = jdbcCatalogFactory.createCatalog(context);
  }

  @Override
  protected Catalog realCatalog() {
    return jdbcCatalog;
  }
}
