/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hadoop.hive.conf.HiveConf;

public class GravitinoHiveCatalogFactoryOptions {

  public static final ConfigOption<String> HIVE_METASTORE_URIS =
      ConfigOptions.key(HiveConf.ConfVars.METASTOREURIS.varname)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The Hive metastore URIs, it is higher priority than hive.metastore.uris in hive-site.xml");
}
