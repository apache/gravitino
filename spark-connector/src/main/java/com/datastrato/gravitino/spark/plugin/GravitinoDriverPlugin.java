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
        String.format("%s:%s, should not empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(metalake),
        String.format(
            "%s:%s, should not empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    catalogManager = GravitinoCatalogManager.createGravitinoCatalogManager(gravitinoUri, metalake);
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
          if (catalog.provider().equalsIgnoreCase("hive")) {
            registerGravitinoHiveCatalog(sparkConf, catalog, sparkCatalogConfigName);
            LOG.info("Register " + catalogName + " to Spark catalog manager");
          } else {
            LOG.info(
                "Skip register "
                    + catalogName
                    + ", because "
                    + catalog.provider()
                    + "is not supported");
          }
        });
  }

  private void registerGravitinoHiveCatalog(
      SparkConf sparkConf, Catalog catalog, String sparkCatalogConfigName) {
    sparkConf.set(sparkCatalogConfigName, GravitinoHiveCatalog.class.getName());
    Preconditions.checkArgument(
        catalog.properties() != null, "Hive Catalog properties should not be null");
    String metastoreUri =
        catalog.properties().get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from catalog properties");
    sparkConf.set(
        sparkCatalogConfigName + "." + GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
  }

  // Todo inject Iceberg extensions
  private void registerSqlExtensions() {}
}
