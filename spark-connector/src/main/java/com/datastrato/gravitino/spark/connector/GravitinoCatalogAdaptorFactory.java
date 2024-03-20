/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.spark.connector.hive.HiveAdaptor;
import com.datastrato.gravitino.spark.connector.iceberg.IcebergAdaptor;
import java.util.Locale;

/**
 * GravitinoCatalogAdaptorFactory creates a specific GravitinoCatalogAdaptor according to the
 * catalog provider.
 */
public class GravitinoCatalogAdaptorFactory {
  public static GravitinoCatalogAdaptor createGravitinoAdaptor(String provider) {
    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        return new HiveAdaptor();
      case "lakehouse-iceberg":
        return new IcebergAdaptor();
      default:
        throw new RuntimeException(String.format("Provider:%s is not supported yet", provider));
    }
  }
}
