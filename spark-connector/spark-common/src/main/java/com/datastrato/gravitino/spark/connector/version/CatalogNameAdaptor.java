/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.version;

import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.package$;
import org.apache.spark.util.VersionUtils$;

public class CatalogNameAdaptor {
  private static final Map<String, String> catalogNames =
      ImmutableMap.of(
          "hive-3.3", "com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark33",
          "hive-3.4", "com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark34",
          "hive-3.5", "com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35",
          "lakehouse-iceberg-3.3",
              "com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark33",
          "lakehouse-iceberg-3.4",
              "com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark34",
          "lakehouse-iceberg-3.5",
              "com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35");

  private static String sparkVersion() {
    return package$.MODULE$.SPARK_VERSION();
  }

  private static String getCatalogName(String provider, int majorVersion, int minorVersion) {
    String key =
        String.format("%s-%d.%d", provider.toLowerCase(Locale.ROOT), majorVersion, minorVersion);
    return catalogNames.get(key);
  }

  public static String getCatalogName(String provider) {
    int majorVersion = VersionUtils$.MODULE$.majorVersion(sparkVersion());
    int minorVersion = VersionUtils$.MODULE$.minorVersion(sparkVersion());
    return getCatalogName(provider, majorVersion, minorVersion);
  }
}
