/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.hive;

import static com.datastrato.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions.IDENTIFIER;

import com.datastrato.gravitino.flink.connector.utils.FactoryUtils;
import com.datastrato.gravitino.flink.connector.utils.PropertyUtils;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Factory for creating instances of {@link GravitinoHiveCatalog}. It will be created by SPI
 * discovery in Flink.
 */
public class GravitinoHiveCatalogFactory implements CatalogFactory {
  private HiveCatalogFactory hiveCatalogFactory;

  @Override
  public Catalog createCatalog(Context context) {
    this.hiveCatalogFactory = new HiveCatalogFactory();
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    helper.validateExcept(
        PropertyUtils.HIVE_PREFIX,
        PropertyUtils.HADOOP_PREFIX,
        PropertyUtils.DFS_PREFIX,
        PropertyUtils.FS_PREFIX);

    String hiveConfDir = helper.getOptions().get(HiveCatalogFactoryOptions.HIVE_CONF_DIR);
    String hadoopConfDir = helper.getOptions().get(HiveCatalogFactoryOptions.HADOOP_CONF_DIR);
    HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir);
    // Put the hadoop properties managed by Gravitino into the hiveConf
    PropertyUtils.getHadoopAndHiveProperties(context.getOptions()).forEach(hiveConf::set);
    return new GravitinoHiveCatalog(
        context.getName(),
        helper.getOptions().get(HiveCatalogFactoryOptions.DEFAULT_DATABASE),
        hiveConf,
        helper.getOptions().get(HiveCatalogFactoryOptions.HIVE_VERSION));
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.<ConfigOption<?>>builder()
        .addAll(hiveCatalogFactory.requiredOptions())
        .add(GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS)
        .build();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return hiveCatalogFactory.optionalOptions();
  }
}
