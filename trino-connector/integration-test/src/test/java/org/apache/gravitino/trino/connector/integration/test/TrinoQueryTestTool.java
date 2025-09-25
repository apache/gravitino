/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector.integration.test;

import java.io.File;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.logging.log4j.util.Strings;

public class TrinoQueryTestTool {

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    try {
      options.addOption(
          "auto",
          true,
          "Start the test containers and gravitino server automatically, the values are 'all','gravitation','none'."
              + "The default value is 'all'. If the value is 'gravitino', only the gravitino server will be started automatically.");

      options.addOption(
          "ignore_failed", false, "Ignore the failed test cases, the default value is 'false'");
      options.addOption(
          "gen_output",
          false,
          "Generate the output file for the test set, the default value is 'false'");

      options.addOption(
          "test_host",
          true,
          "Host address for all test services (include gravitino server, trino, hive, postgresql, mysql..., "
              + "if the services are not running on the same host. set the arguments like --gravitino_uri=xxx and --trino_uri=xxx), "
              + "all test services use 127.0.0.1 as default, if --auto is set to 'all', this option is ignored");
      options.addOption(
          "gravitino_uri",
          true,
          "URL for Gravitino server, if --auto is set to 'all', this option is ignored");
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
          "test_sets_dir",
          true,
          "Specify the test sets' directory, "
              + "the default value is 'integration-test/src/test/resources/trino-queries'");
      options.addOption("test_set", true, "Specify the test set name to test");
      options.addOption("tester_id", true, "Specify the tester file name prefix to select to test");
      options.addOption("catalog", true, "Specify the catalog name to test");

      options.addOption(
          "params",
          true,
          "Additional parameters that can replace the value of ${key} in the testers contents, "
              + "example: --params=key1,v1;key2,v2");

      options.addOption(
          "trino_worker_num",
          true,
          "Specify the number of Trino independent worker, the default value is 0."
              + "Use a distributed cluster for integration testing when necessary (value > 0), "
              + "otherwise fall back to a single-node setup with combined coordinator-worker roles.");

      options.addOption("help", false, "Print this help message");

      CommandLineParser parser = new PosixParser();
      CommandLine commandLine = parser.parse(options, args);

      if (commandLine.hasOption("help")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("TrinoTestTool", options);
        String example =
            "Examples:\n"
                + "Run all the testers in the 'testsets' directory:\n"
                + "TrinoTestTool --auto=all\n\n"
                + "Run all the testers in the 'testsets' directory with a distributed cluster:\n"
                + "TrinoTestTool --auto=all --trino_worker_num=3\n\n"
                + "Run all the tpch testset's testers in the 'testsets/tpch' directory:\n"
                + "TrinoTestTool --testset=tpch --auto=all\n\n"
                + "Run the tester 'testsets/tpch/00005.sql' in the tpch testset under hive catalog :\n"
                + "TrinoTestTool --testset=tpch --tester_id=00005 --catalog=hive --auto=all\n\n"
                + "Run all the tpch testset's testers in the 'testsets/tpch' directory under 'mysql' "
                + "catalog with manual start the test environment:\n"
                + "TrinoTestTool --testset=tpch -- catalog=mysql --auto=none --gravitino_uri=http://10.3.21.12:8090 "
                + "--trino_uri=http://10.3.21.12:8080 --mysql_url=jdbc:mysql:/10.3.21.12 \n";
        System.out.println(example);
        return;
      }

      boolean autoStart = true;
      boolean autoStartGravitino = true;
      if (commandLine.getOptionValue("auto") != null) {
        String auto = commandLine.getOptionValue("auto");
        switch (auto) {
          case "all":
            break;
          case "gravitino":
            autoStart = false;
            break;
          case "none":
            autoStart = false;
            autoStartGravitino = false;
            break;
          default:
            System.out.println("The value of --auto must be 'all', 'gravitino' or 'none'");
            System.exit(1);
        }
      }

      if (commandLine.hasOption("ignore_failed")) {
        TrinoQueryIT.exitOnFailed = false;
      }

      TrinoQueryIT.ciTestsets.clear();

      String testHost = commandLine.getOptionValue("test_host");
      if (Strings.isNotEmpty(testHost)) {
        TrinoQueryIT.testHost = testHost;
        TrinoQueryIT.gravitinoUri = String.format("http://%s:8090", testHost);
        TrinoQueryIT.trinoUri = String.format("http://%s:8080", testHost);
        TrinoQueryIT.hiveMetastoreUri = String.format("thrift://%s:9083", testHost);
        TrinoQueryIT.hdfsUri = String.format("hdfs://%s:9000", testHost);
        TrinoQueryIT.mysqlUri = String.format("jdbc:mysql://%s", testHost);
        TrinoQueryIT.postgresqlUri = String.format("jdbc:postgresql://%s", testHost);
      }

      String gravitinoUri = commandLine.getOptionValue("gravitino_uri");
      TrinoQueryIT.gravitinoUri =
          Strings.isBlank(gravitinoUri) ? TrinoQueryIT.gravitinoUri : gravitinoUri;
      String trinoUri = commandLine.getOptionValue("trino_uri");
      TrinoQueryIT.trinoUri = Strings.isBlank(trinoUri) ? TrinoQueryIT.trinoUri : trinoUri;
      String hiveUri = commandLine.getOptionValue("hive_uri");
      TrinoQueryIT.hiveMetastoreUri =
          Strings.isBlank(hiveUri) ? TrinoQueryIT.hiveMetastoreUri : hiveUri;
      String mysqlUri = commandLine.getOptionValue("mysql_uri");
      TrinoQueryIT.mysqlUri = Strings.isBlank(mysqlUri) ? TrinoQueryIT.mysqlUri : mysqlUri;
      String hdfsUri = commandLine.getOptionValue("hdfs_uri");
      TrinoQueryIT.hdfsUri = Strings.isBlank(hdfsUri) ? TrinoQueryIT.hdfsUri : hdfsUri;
      String postgresqlUri = commandLine.getOptionValue("postgresql_uri");
      TrinoQueryIT.postgresqlUri =
          Strings.isBlank(postgresqlUri) ? TrinoQueryIT.postgresqlUri : postgresqlUri;

      String params = commandLine.getOptionValue("params");
      if (Strings.isNotBlank(params)) {
        for (String param : params.split(";")) {
          String[] split = param.split(",");
          String key = split[0].trim();
          String value = split[1].trim();
          if (Strings.isNotBlank(key) && Strings.isNotBlank(value)) {
            TrinoQueryIT.queryParams.put(key, value);
          } else {
            System.out.println(
                "Invalid parameters:" + param + "The format should like --params=key1,v1;key2,v2");
            System.exit(1);
          }
        }
      }

      String testSet = commandLine.getOptionValue("test_set");
      String testerId = commandLine.getOptionValue("tester_id", "");
      String catalog = commandLine.getOptionValue("catalog", "");
      String testSetsDir = commandLine.getOptionValue("test_sets_dir", "");

      if (testSetsDir.isEmpty()) {
        testSetsDir = TrinoQueryIT.class.getClassLoader().getResource("trino-ci-testset").getPath();
        testSetsDir = ITUtils.joinPath(testSetsDir, "testsets");
      } else {
        TrinoQueryIT.testsetsDir = testSetsDir;
      }

      String testSetDir = ITUtils.joinPath(testSetsDir, testSet);
      if (testSet != null) {
        if (!new File(testSetDir).exists()) {
          System.out.println("The test set directory " + testSetDir + " does not exist");
          System.exit(1);
        }
        if (Strings.isNotEmpty(catalog)) {
          if (!new File(ITUtils.joinPath(testSetDir, "catalog_" + catalog + "_prepare.sql"))
              .exists()) {
            System.out.println("The catalog " + catalog + " does not found in testset");
            System.exit(1);
          }
        }
        if (Strings.isNotEmpty(testerId)) {
          if (Arrays.stream(TrinoQueryIT.listDirectory(testSetDir))
              .noneMatch(f -> f.startsWith(testerId))) {
            System.out.println("The tester " + testerId + " does not found in testset");
            System.exit(1);
          }
        }
      }

      String trinoWorkerNumConfig = commandLine.getOptionValue("trino_worker_num", "0");
      try {
        int trinoWorkerNum = Integer.parseInt(trinoWorkerNumConfig);
        if (trinoWorkerNum < 0) {
          System.out.println(
              "The value of trino_worker_num must be greater than zero, current value is: "
                  + trinoWorkerNumConfig);
          System.exit(1);
        }
        TrinoQueryIT.trinoWorkerNum = trinoWorkerNum;
      } catch (Exception e) {
        System.out.println(
            "The value of trino_worker_num must be an integer value, current value is: "
                + trinoWorkerNumConfig);
        System.exit(1);
      }

      checkEnv();

      TrinoQueryITBase.autoStart = autoStart;
      TrinoQueryITBase.autoStartGravitino = autoStartGravitino;

      TrinoQueryIT testerRunner = new TrinoQueryIT();
      testerRunner.setup();

      if (commandLine.hasOption("gen_output")) {
        String catalogFileName = "catalog_" + catalog + "_prepare.sql";
        testerRunner.runOneTestSetAndGenOutput(testSetDir, catalogFileName, testerId);
        System.out.println("The output file is generated successfully in the path " + testSetDir);
        return;
      }

      if (testSet == null) {
        testerRunner.testSql();
      } else {
        testerRunner.testSql(testSetDir, catalog, testerId);
      }
      System.out.printf(
          "All testers have finished. Total:%s, Pass: %s\n%s",
          testerRunner.totalCount, testerRunner.passCount, testerRunner.generateTestStatus());
    } catch (Exception e) {
      System.out.println(e.getMessage());
      System.exit(-1);
    } finally {
      TrinoQueryIT.cleanup();
    }
  }

  private static void checkEnv() {
    if (Strings.isEmpty(System.getenv("GRAVITINO_ROOT_DIR"))) {
      throw new RuntimeException("GRAVITINO_ROOT_DIR is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_HOME"))) {
      throw new RuntimeException("GRAVITINO_HOME is not set");
    }

    if (Strings.isEmpty(System.getenv("GRAVITINO_TEST"))) {
      throw new RuntimeException("GRAVITINO_TEST is not set, please set it to 'true'");
    }
  }
}
