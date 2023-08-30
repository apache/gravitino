/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class HiveCatalogConfig extends Config {
  public static final ConfigEntry<String> HADOOP_USER_NAME =
      new ConfigBuilder("graviton.hadoop.user.name")
          .doc(
              "The specify Hadoop user name that will be used when accessing Hadoop Distributed File System (HDFS).")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("hive");
}
