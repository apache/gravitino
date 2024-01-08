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
    String gravitinoUrl = conf.get(GravitinoSparkConfig.GRAVITINO_URL);
    String metalake = conf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(gravitinoUrl) && StringUtils.isNoneBlank(metalake),
        String.format(""));
    catalogManager = GravitinoCatalogManager.createCatalogManager(gravitinoUrl, metalake);
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
          if (catalog.provider().equalsIgnoreCase("hive")) {
            Preconditions.checkArgument(
                sparkConf.contains(sparkCatalogConfigName) == false,
                catalogName + " is already registered to SparkCatalogManager");
            sparkConf.set(sparkCatalogConfigName, GravitinoHiveCatalog.class.getName());
            String metastoreUri = catalog.properties().get("metastore.uris");
            sparkConf.set(sparkCatalogConfigName + ".hive.metastore.uris", metastoreUri);
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

  // todo inject Iceberg extensions
  private void registerSqlExtensions() {}
}
