/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.hive;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.combine.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;

/** Override writeNewMetadataIfRequired not store metadata in secondary catalog */
public class CombineHiveTableOperations extends HiveTableOperations {
  protected CombineHiveTableOperations(
      Configuration conf,
      ClientPool metaClients,
      FileIO fileIO,
      String catalogName,
      String database,
      String table) {
    super(conf, metaClients, fileIO, catalogName, database, table);
  }

  @Override
  protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
    return Utils.resueMetadataLocation(metadata);
  }
}
