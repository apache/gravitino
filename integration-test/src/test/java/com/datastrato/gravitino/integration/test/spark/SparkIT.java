/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class SparkIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(SparkIT.class);
  private static String HIVE_METASTORE_URIS = "thrift://127.0.0.1:9083";
  // private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static SparkSession sparkSession;
  private static FileSystem hdfs;

  @BeforeAll
  public static void startup() throws Exception {

    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Spark integration test")
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config(
                "spark.sql.warehouse.dir",
                String.format("hdfs://127.0.0.1:9000/user/hive/spark-warehouse"))
            .config("spark.sql.storeAssignmentPolicy", "LEGACY")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
            .enableHiveSupport()
            .getOrCreate();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", String.format("hdfs://127.0.0.1:9000"));
    hdfs = FileSystem.get(conf);
  }

  @Test
  public void testSpark() {
    sparkSession.sql(
        "CREATE TABLE if NOT EXISTS sales ( id INT, name STRING, age INT ) PARTITIONED BY (country STRING, state STRING)");
    sparkSession.sql("desc table extended sales").show();
    sparkSession.sql(
        "INSERT INTO sales PARTITION (country='USA', state='CA') VALUES (1, 'John', 25);");
    sparkSession.sql(
        "INSERT INTO sales PARTITION (country='Canada', state='ON') VALUES (2, 'Alice', 30);");
    sparkSession.sql("select * from sales where country = 'USA'").explain("formatted");

    // insert into select xx
    sparkSession.sql(
        "CREATE TABLE IF NOT EXISTS target_table (id INT, name STRING, age INT) PARTITIONED BY (p INT)");
    sparkSession
        .sql(
            "INSERT INTO target_table PARTITION ( p = 1 ) SELECT id, name, age FROM sales WHERE country='USA' AND state='CA'")
        .explain("extended");
    sparkSession.sql("select * from target_table").show();

    // create table as select
    // sparkSession.sql("CREATE TABLE IF NOT EXISTS target_table2 as select * from
    // sales").explain("formatted");
    // sparkSession.sql("CREATE TABLE IF NOT EXISTS target_table2 as select * from
    // sales").explain("extended");
  }

  @AfterAll
  public static void stop() throws IOException {

    if (sparkSession != null) {
      sparkSession.close();
    }

    if (hdfs != null) {
      hdfs.close();
    }
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
  }
}
