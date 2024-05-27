/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.hive;

import com.datastrato.gravitino.flink.connector.catalog.BaseCatalog;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.factories.Factory;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * The GravitinoHiveCatalog class is a implementation of the BaseCatalog class that is used to proxy
 * the HiveCatalog class.
 */
public class GravitinoHiveCatalog extends BaseCatalog {

  private HiveCatalog hiveCatalog;

  GravitinoHiveCatalog(
      String catalogName,
      String defaultDatabase,
      @Nullable HiveConf hiveConf,
      @Nullable String hiveVersion) {
    super(catalogName, defaultDatabase);
    this.hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConf, hiveVersion);
  }

  public HiveConf getHiveConf() {
    return hiveCatalog.getHiveConf();
  }

  @Override
  public Optional<Factory> getFactory() {
    return hiveCatalog.getFactory();
  }
}
