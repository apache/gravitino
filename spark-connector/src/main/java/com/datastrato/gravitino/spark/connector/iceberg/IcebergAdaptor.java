package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.spark.connector.GravitinoCatalogAdaptor;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.table.SparkBaseTable;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** IcebergAdaptor provides specific operations for Iceberg Catalog to adapt to GravitinoCatalog. */
public class IcebergAdaptor implements GravitinoCatalogAdaptor {

  @Override
  public PropertiesConverter getPropertiesConverter() {
    return new IcebergPropertiesConverter();
  }

  @Override
  public SparkBaseTable createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter) {
    return new SparkIcebergTable(identifier, gravitinoTable, sparkCatalog, propertiesConverter);
  }

  @Override
  public TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");
    String metastoreUri = properties.get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from iceberg catalog properties");

    TableCatalog icebergCatalog = new SparkCatalog();
    HashMap<String, String> all = new HashMap<>(options);
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    icebergCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return icebergCatalog;
  }
}
