/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark;

import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-it")
public class SparkIT extends SparkBaseIT {

  @BeforeEach
  void init() {
    sparkSession.sql("use " + hiveCatalogName);
  }

  @Test
  public void testLoadCatalogs() {
    Set<String> catalogs = convertToStringSet(sql("show catalogs"), 0);
    catalogs.forEach(k -> System.out.println(k));
    Assertions.assertTrue(catalogs.contains(hiveCatalogName));
  }

  public void testFunction() {
    sparkSession.sql("select current_date(), unix_timestamp();").show();
  }

  public void testView() {
    sparkSession.sql("create database if not exists v");
    sparkSession.sql("use f");
    // sparkSession.sql("create GLOBAL TEMPORARY VIEW IF NOT EXISTS view_student as select * from
    // student limit 2;");
    // sparkSession.sql("create view view_student1 as select * from student limit 2;");
    // sparkSession.sql("select * from view_student1").show();
  }

  public void testFederatinoQuery() {
    sparkSession.sql("use hive");
    sparkSession.sql("create database if not exists f");
    sparkSession.sql("drop table if exists f.student");
    sparkSession.sql("CREATE TABLE f.student (id INT, name STRING, age INT)").show();
    sparkSession.sql("INSERT into f.student VALUES(0, 'aa', 10), (1,'bb', 12);").show();
    sparkSession.sql("desc table EXTENDED f.student").show();

    sparkSession.sql("create database if not exists hive1.f1");
    sparkSession.sql("drop table if exists hive1.f1.scores");
    sparkSession.sql("CREATE TABLE hive1.f1.scores (id INT, score INT)").show();
    sparkSession.sql("desc table EXTENDED hive1.f1.scores").show();
    sparkSession.sql("INSERT into hive1.f1.scores VALUES(0, 100), (1, 98)").show();

    sparkSession
        .sql(
            "select f.student.id, name, age, score from hive.f.student JOIN hive1.f1.scores ON f.student.id = hive1.f1.scores.id")
        .show();
  }

  public void testTestCreateDatabase() {
    sparkSession.sql("create database if not exists db_create2");
    sparkSession.sql("show databases").show();
  }

  public void testCreateDatasourceTable() {
    sparkSession
        .sql(
            "CREATE TABLE student_parquet(id INT, name STRING, age INT) USING PARQUET"
                + " OPTIONS ('parquet.bloom.filter.enabled'='true', "
                + "'parquet.bloom.filter.enabled#age'='false');")
        .show();
  }

  public void testCreateHiveTable() {
    sparkSession.sql("use default");
    sparkSession.sql("drop table if exists student");
    sparkSession.sql("drop table if exists student1");
    sparkSession.sql(
        "CREATE TABLE default.student (id INT, name STRING, age INT)\n"
            //     + "    USING CSV\n"
            + "    PARTITIONED BY (age)\n"
            + "    CLUSTERED BY (Id) SORTED BY (name) INTO 4 buckets ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n"
            + "    STORED AS TEXTFILE TBLPROPERTIES ('foo'='bar')\n"
            + "    LOCATION '/tmp/family/' \n"
            + "    COMMENT 'this is a comment';");
    sparkSession.sql("create table student1 as select * from default.student").show();
  }

  public void testHiveDML() {
    sparkSession.sql("create database if not exists db");
    sparkSession.sql("drop table if exists db.student");
    sparkSession.sql("use db");
    sparkSession.sql("CREATE TABLE student (id INT, name STRING, age INT)").show();
    sparkSession.sql("desc db.student").show();
    sparkSession.sql("INSERT into db.student VALUES(0, 'aa', 10), (1,'bb', 12);").show();
    sparkSession.sql("drop table if exists db.student1");
    sparkSession.sql("create table db.student1 as select * from db.student limit 1");
    sparkSession.sql("INSERT into db.student1 select * from db.student limit 1");
    sparkSession.sql("select * from db.student1;").show();
  }

  /*
  @Test
  public void testSpark() {
    sparkSession.sql(
        "CREATE TABLE if NOT EXISTS sales ( id INT, name STRING, age INT ) PARTITIONED BY (country STRING, state STRING)");
    sparkSession.sql("desc table extended sales").show();
    sparkSession.sql(
        "INSERT INTO sales PARTITION (country='USA', state='CA') VALUES (1, 'John', 25);");
    sparkSession
        .sql("INSERT INTO sales PARTITION (country='Canada', state='ON') VALUES (2, 'Alice', 30);")
        .explain("extended");
    // sparkSession.sql("select * from sales where country = 'USA'").explain("extended");

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
    sparkSession
        .sql("CREATE TABLE IF NOT EXISTS target_table2 as select * from sales")
        .explain("extended");
  }
   */
}
