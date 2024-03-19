/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkHiveCatalogIT extends SparkCommonIT {

  // Use a custom database not the original default database because SparkHiveCatalogIT couldn't
  // read&write
  // data to tables in default database. The main reason is default database location is
  // determined by `hive.metastore.warehouse.dir` in hive-site.xml which is local HDFS address
  // not real HDFS address. The location of tables created under default database is like
  // hdfs://localhost:9000/xxx which couldn't read write data from SparkHiveCatalogIT. Will use
  // default
  // database after spark connector support Alter database xx set location command.
  @BeforeAll
  void initHiveCatalog() {
    startUp("hive", "hive");
    sql("USE " + catalogName);
    createDatabaseIfNotExists(getDefaultDatabase());
  }

  @BeforeEach
  void init() {
    sql("USE " + catalogName);
    sql("USE " + getDefaultDatabase());
  }
}
