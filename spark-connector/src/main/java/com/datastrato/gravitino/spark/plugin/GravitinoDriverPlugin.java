/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.plugin;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.catalog.GravitinoCatalogManager;
import com.datastrato.gravitino.spark.catalog.GravitinoHiveCatalog;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Gravitino and
 * register Gravitino catalogs to Spark.
 */
public class GravitinoDriverPlugin implements DriverPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);

  private GravitinoCatalogManager catalogManager;

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    SparkConf conf = sc.conf();
    String gravitinoUri = conf.get(GravitinoSparkConfig.GRAVITINO_URI);
    String metalake = conf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    catalogManager = GravitinoCatalogManager.create(gravitinoUri, metalake);
    catalogManager.loadCatalogsFromGravitino();
    registerGravitinoCatalogs(conf, catalogManager.getGravitinoCatalogs());
    registerSqlExtensions();
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  private void registerGravitinoCatalogs(SparkConf sparkConf, Map<String, Catalog> catalogs) {
    catalogs.forEach(
        (catalogName, catalog) -> {
          String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
          Preconditions.checkArgument(
              sparkConf.contains(sparkCatalogConfigName) == false,
              catalogName + " is already registered to SparkCatalogManager");
          String sparkCatalogClassName = getCatalogClassName(catalog.provider());
          if (sparkCatalogClassName != null) {
            sparkConf.set(sparkCatalogConfigName, sparkCatalogClassName);
            LOG.info("Register {} catalog to Spark catalog manager", catalogName);
          } else {
            LOG.info(
                "Skip register {} catalog, because {} is not supported",
                catalogName,
                catalog.provider());
          }
        });
  }

  private String getCatalogClassName(String provider) {
    if (provider == null) {
      return null;
    }

    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        return GravitinoHiveCatalog.class.getName();
      default:
        return null;
    }
  }

  // Todo inject Iceberg extensions
  private void registerSqlExtensions() {}
}
