/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.plugin;

import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.catalog.GravitinoCatalog;
import com.datastrato.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
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
        StringUtils.isNotBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    catalogManager = GravitinoCatalogManager.create(gravitinoUri, metalake);
    Set<String> catalogs = catalogManager.listCatalogs();
    registerGravitinoCatalogs(conf, catalogs);
    registerSqlExtensions();
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  private void registerGravitinoCatalogs(SparkConf sparkConf, Set<String> catalogNames) {
    catalogNames.forEach(
        catalogName -> {
          String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
          Preconditions.checkArgument(
              !sparkConf.contains(sparkCatalogConfigName),
              catalogName + " is already registered to SparkCatalogManager");
          sparkConf.set(sparkCatalogConfigName, GravitinoCatalog.class.getName());
          LOG.info("Register {} catalog to Spark catalog manager", catalogName);
        });
  }

  // Todo inject Iceberg extensions
  private void registerSqlExtensions() {}
}
