/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import java.io.File;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.logging.log4j.util.Strings;

public class TrinoQueryTestTool {

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    try {
      options.addOption(
          "auto",
          true,
          "Start the test containers and gravitino server automatically, the default value is 'all'. "
              + "If the value is 'gravitino', only the gravitino server will be started automatically.");

      options.addOption(
          "gen_output",
          true,
          "Generate the output file for the test set, the default value is 'false'");

      options.addOption(
          "trino_uri", true, "URL for Trino, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "hive_uri", true, "URL for Hive, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "mysql_uri", true, "URL for MySQL, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "hdfs_uri", true, "URL for HDFS, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "postgresql_uri",
          true,
          "URL for PostgreSQL, if --auto is set to 'all', this option is ignored");

      options.addOption(
          "test_set_dir",
          true,
          "Specify the test set directory, "
              + "the default value is 'integration-test/src/test/resources/trino-queries'");
      options.addOption("test_set", true, "Specify the test set name to test");
      options.addOption("tester_id", true, "Specify the tester name prefix to select to test");
      options.addOption("catalog", true, "Specify the catalog name to test");

      CommandLineParser parser = new PosixParser();
      CommandLine commandLine = parser.parse(options, args);

      boolean autoStart = true;
      boolean autoStartGravitino = true;
      if (commandLine.getOptionValue("auto") != null) {
        String auto = commandLine.getOptionValue("auto");
        if (auto.equals("all")) {
          autoStart = true;
          autoStartGravitino = true;
        } else if (auto.equals("gravitino")) {
          autoStart = false;
          autoStartGravitino = true;
        } else if (auto.equals("none")){
          autoStart = false;
          autoStartGravitino = false;
        } else {
          System.out.println("The value of --auto must be 'all' or 'gravitino'");
          return;
        }
      }

      String trinoUri = commandLine.getOptionValue("trino_uri");
      TrinoQueryITBase.trinoUri = Strings.isBlank(trinoUri) ? TrinoQueryITBase.trinoUri : trinoUri;
      String hiveUri = commandLine.getOptionValue("hive_uri");
      TrinoQueryITBase.hiveMetastoreUri =
          Strings.isBlank(hiveUri) ? TrinoQueryITBase.hiveMetastoreUri : hiveUri;
      String mysqlUri = commandLine.getOptionValue("mysql_uri");
      TrinoQueryITBase.mysqlUri = Strings.isBlank(mysqlUri) ? TrinoQueryITBase.mysqlUri : mysqlUri;
      String hdfsUri = commandLine.getOptionValue("hdfs_uri");
      TrinoQueryITBase.hdfsUri = Strings.isBlank(hdfsUri) ? TrinoQueryITBase.hdfsUri : hdfsUri;
      String postgresqlUri = commandLine.getOptionValue("postgresql_uri");
      TrinoQueryITBase.postgresqlUri =
          Strings.isBlank(postgresqlUri) ? TrinoQueryITBase.postgresqlUri : postgresqlUri;

      String testSet = commandLine.getOptionValue("test_set");
      String testerId = commandLine.getOptionValue("tester_id", "");
      String catalog = commandLine.getOptionValue("catalog", "");
      String testSetDir = commandLine.getOptionValue("test_set_dir", "");

      if (testSetDir.isEmpty()) {
        testSetDir = TrinoQueryIT.class.getClassLoader().getResource("trino-ci-testset").getPath();
        testSetDir = testSetDir + "/testsets";
      }
      TrinoQueryIT2.testsetsDir = testSetDir;
      String path = TrinoQueryIT2.testsetsDir + "/" + testSet;

      if (testSet != null) {
        if (!new File(path).exists()) {
          System.out.println("The test set directory " + path + " does not exist");
          System.exit(1);
        }
        if (Strings.isNotEmpty(testerId)) {
          if (Arrays.stream(TrinoQueryITBase.listDirectory(path))
                  .filter(f -> f.startsWith(testerId))
                  .count()
              == 0) {
            System.out.println("The tester " + testerId + " does not found in testset");
          }
        }
      }

      TrinoQueryITBase.autoStart = autoStart;
      TrinoQueryITBase.autoStartGravition = autoStartGravitino;

      TrinoQueryIT2.setup();
      TrinoQueryIT2 testerRunner = new TrinoQueryIT2();

      String gen = commandLine.getOptionValue("gen_output");
      if (Strings.isNotEmpty(gen)) {
        testerRunner.runOneTestSetAndGenOutput(path, "catalog_tpcds_prepare.sql", testerId);
        System.out.println("The output file is generated successfully in the path " + path);
        return;
      }

      if (testSet == null) {
        testerRunner.testSql();
      } else {
        testerRunner.testSql(testSetDir + "/" + testSet, catalog, testerId);
      }
      System.out.println("All the testers completed");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      TrinoQueryIT2.cleanup();
    }
  }
}
