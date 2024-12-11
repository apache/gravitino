package org.apache.gravitino.flink.connector.paimon;

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;

/**
 * The GravitinoPaimonCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the PaimonCatalog class.
 */
public class GravitinoPaimonCatalog extends BaseCatalog {

  private AbstractCatalog paimonCatalog;

  protected GravitinoPaimonCatalog(String catalogName, AbstractCatalog paimonCatalog) {
    super(catalogName, paimonCatalog.getDefaultDatabase());
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return paimonCatalog;
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return PaimonPropertiesConverter.INSTANCE;
  }

  @Override
  protected PartitionConverter getPartitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }
}
