/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.plugin;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalog;
import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
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
        StringUtils.isNotBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    catalogManager = GravitinoCatalogManager.create(gravitinoUri, metalake);
    catalogManager.loadRelationalCatalogs();
    registerGravitinoCatalogs(conf, catalogManager.getCatalogs());
    registerSqlExtensions();
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  private void registerGravitinoCatalogs(
      SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    gravitinoCatalogs
        .entrySet()
        .forEach(
            entry -> {
              String catalogName = entry.getKey();
              Catalog gravitinoCatalog = entry.getValue();
              String provider = gravitinoCatalog.provider();
              try {
                registerCatalog(sparkConf, catalogName, provider);
              } catch (Exception e) {
                LOG.warn("Register catalog {} failed.", catalogName, e);
              }
            });
  }

  private void registerCatalog(SparkConf sparkConf, String catalogName, String provider) {
    if (StringUtils.isBlank(provider)) {
      LOG.warn("Skip registering {} because catalog provider is empty.", catalogName);
      return;
    }

    String catalogClassName;
    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        catalogClassName = GravitinoHiveCatalog.class.getName();
        break;
      case "lakehouse-iceberg":
        catalogClassName = GravitinoIcebergCatalog.class.getName();
        break;
      default:
        LOG.warn("Skip registering {} because {} is not supported yet.", catalogName, provider);
        return;
    }

    String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    sparkConf.set(sparkCatalogConfigName, catalogClassName);
    LOG.info("Register {} catalog to Spark catalog manager.", catalogName);
  }

  // Todo inject Iceberg extensions
  private void registerSqlExtensions() {}
}
