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
package org.apache.gravitino.spark.connector.integration.test.sql;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.junit.platform.commons.util.StringUtils;

public class SparkTestConfig extends Config {
  private static final String DEFAULT_BASE_DIR =
      Paths.get(
              System.getenv("GRAVITINO_ROOT_DIR"),
              "spark-connector",
              "spark-common",
              "src",
              "test",
              "resources")
          .toString();

  private static final ConfigEntry<String> TEST_BASE_DIR =
      new ConfigBuilder("gravitino.spark.test.dir")
          .doc("The Spark SQL test base dir")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault(DEFAULT_BASE_DIR);

  private static final ConfigEntry<String> TEST_SQLS =
      new ConfigBuilder("gravitino.spark.test.sqls")
          .doc(
              "Specify the test SQLs, using directory to specify group of SQLs like "
                  + "`test-sqls/hive`, using file path to specify one SQL like "
                  + "`test-sqls/hive/basic.sql`, use `,` to split multi part")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  private static final ConfigEntry<Boolean> GENERATE_GOLDEN_FILES =
      new ConfigBuilder("gravitino.spark.test.generateGoldenFiles")
          .doc(
              "Whether generate golden files which are used to check the correctness of the SQL result")
          .version(ConfigConstants.VERSION_0_6_0)
          .booleanConf()
          .createWithDefault(Boolean.FALSE);

  private static final ConfigEntry<String> GRAVITINO_METALAKE_NAME =
      new ConfigBuilder("gravitino.spark.test.metalake")
          .doc("The metalake name to run the test")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault("test");

  private static final ConfigEntry<Boolean> SETUP_GRAVITINO_ENV =
      new ConfigBuilder("gravitino.spark.test.setupEnv")
          .doc("Whether to setup Gravitino and Hive environment")
          .version(ConfigConstants.VERSION_0_6_0)
          .booleanConf()
          .createWithDefault(Boolean.FALSE);

  private static final ConfigEntry<String> GRAVITINO_URI =
      new ConfigBuilder("gravitino.spark.test.uri")
          .doc(
              "Gravitino uri address, only available when `gravitino.spark.test.setupEnv` is false")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault("http://127.0.0.1:8090");

  private static final ConfigEntry<String> ICEBERG_WAREHOUSE_DIR =
      new ConfigBuilder("gravitino.spark.test.iceberg.warehouse")
          .doc(
              "Iceberg warehouse location, only available when `gravitino.spark.test.setupEnv` is false")
          .version("0.6.0")
          .stringConf()
          .createWithDefault("hdfs://127.0.0.1:9000/user/hive/warehouse-spark-test");

  public String getBaseDir() {
    return get(TEST_BASE_DIR);
  }

  public boolean generateGoldenFiles() {
    return get(GENERATE_GOLDEN_FILES);
  }

  public boolean isGravitinoEnvSetUp() {
    return get(SETUP_GRAVITINO_ENV);
  }

  public List<String> getTestSQLs() {
    String testSQLs = get(TEST_SQLS);
    if (StringUtils.isNotBlank(testSQLs)) {
      return Arrays.asList(testSQLs.split("\\s*,\\s*"));
    }
    return new ArrayList<>();
  }

  public String getGravitinoUri() {
    return get(GRAVITINO_URI);
  }

  public String getMetalakeName() {
    return get(GRAVITINO_METALAKE_NAME);
  }

  public String getWarehouseLocation() {
    return get(ICEBERG_WAREHOUSE_DIR);
  }
}
