/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.plugin;

import static com.datastrato.gravitino.spark.connector.ConnectorConstants.COMMA;
import static com.datastrato.gravitino.spark.connector.utils.ConnectorUtil.removeDuplicateSparkExtensions;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.spark.connector.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import com.datastrato.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import com.datastrato.gravitino.spark.connector.version.CatalogNameAdaptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Gravitino and
 * register Gravitino catalogs to Spark.
 */
public class GravitinoDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);

  private GravitinoCatalogManager catalogManager;
  private final List<String> toRegisteredDriverExtensions =
      Arrays.asList(GravitinoIcebergSparkSessionExtensions.class.getName());
  private final List<String> gravitinoDriverExtensions = new ArrayList<>();
  private boolean enableIcebergSupport = false;

  @VisibleForTesting
  static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

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

    gravitinoDriverExtensions.addAll(toRegisteredDriverExtensions);

    this.enableIcebergSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, false);
    if (enableIcebergSupport) {
      gravitinoDriverExtensions.add(ICEBERG_SPARK_EXTENSIONS);
    }

    this.catalogManager = GravitinoCatalogManager.create(gravitinoUri, metalake);
    catalogManager.loadRelationalCatalogs();
    registerGravitinoCatalogs(conf, catalogManager.getCatalogs());
    registerSqlExtensions(conf);
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
              if ("lakehouse-iceberg".equals(provider.toLowerCase(Locale.ROOT))
                  && enableIcebergSupport == false) {
                return;
              }
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

    String catalogClassName = CatalogNameAdaptor.getCatalogName(provider);
    if (StringUtils.isBlank(catalogClassName)) {
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

  private void registerSqlExtensions(SparkConf conf) {
    String extensionString = String.join(COMMA, gravitinoDriverExtensions);
    if (conf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key())) {
      String sparkSessionExtensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
      if (StringUtils.isNotBlank(sparkSessionExtensions)) {
        conf.set(
            StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
            removeDuplicateSparkExtensions(
                gravitinoDriverExtensions.toArray(new String[0]),
                sparkSessionExtensions.split(COMMA)));
      } else {
        conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
      }
    } else {
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
    }
  }
}
