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
package org.apache.gravitino.catalog.hive.integration.test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

// Apart from the following dependencies on environment, this test also needs hadoop3-common, please
// refer to L135 in the file `${GRAVITINO_HOME}/catalogs/catalog-hive/build.gradle.kts`, otherwise
// initFileSystem method in this file will fail to run due to missing hadoop3-common.
@EnabledIf(
    value = "isAzureBlobStorageConfigured",
    disabledReason = "Azure Blob Storage is not prepared.")
public class CatalogHiveABSIT extends CatalogHiveIT {

  private static final String ABS_BUCKET_NAME = System.getenv("ABS_CONTAINER_NAME");
  private static final String ABS_USER_ACCOUNT_NAME = System.getenv("ABS_ACCOUNT_NAME");
  private static final String ABS_USER_ACCOUNT_KEY = System.getenv("ABS_ACCOUNT_KEY");

  @Override
  protected void startNecessaryContainer() {
    Map<String, String> hiveContainerEnv =
        ImmutableMap.of(
            "ABS_ACCOUNT_NAME",
            ABS_USER_ACCOUNT_NAME,
            "ABS_ACCOUNT_KEY",
            ABS_USER_ACCOUNT_KEY,
            HiveContainer.HIVE_RUNTIME_VERSION,
            HiveContainer.HIVE3);

    containerSuite.startHiveContainerWithS3(hiveContainerEnv);

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainerWithS3().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  @Override
  protected void initFileSystem() throws IOException {
    // Use Azure Blob Storage file system
    Configuration conf = new Configuration();
    conf.set(
        String.format("fs.azure.account.key.%s.dfs.core.windows.net", ABS_USER_ACCOUNT_NAME),
        ABS_USER_ACCOUNT_KEY);
    conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");

    String path =
        String.format("abfss://%s@%s.dfs.core.windows.net", ABS_BUCKET_NAME, ABS_USER_ACCOUNT_NAME);
    fileSystem = FileSystem.get(URI.create(path), conf);
  }

  @Override
  protected void initSparkSession() {
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Hive Catalog integration test")
            .config("hive.metastore.uris", HIVE_METASTORE_URIS)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "abfss://%s@%s.dfs.core.windows.net/%s",
                    ABS_BUCKET_NAME,
                    ABS_USER_ACCOUNT_NAME,
                    GravitinoITUtils.genRandomName("CatalogFilesetIT")))
            .config(
                String.format(
                    "spark.hadoop.fs.azure.account.key.%s.dfs.core.windows.net",
                    ABS_USER_ACCOUNT_NAME),
                ABS_USER_ACCOUNT_KEY)
            .config("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem")
            .config("spark.sql.storeAssignmentPolicy", "LEGACY")
            .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
            .enableHiveSupport()
            .getOrCreate();
  }

  @Override
  protected Map<String, String> createSchemaProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    properties.put(
        "location",
        String.format(
            "abfss://%s@%s.dfs.core.windows.net/test-%s",
            ABS_BUCKET_NAME, ABS_USER_ACCOUNT_NAME, System.currentTimeMillis()));
    return properties;
  }

  private static boolean isAzureBlobStorageConfigured() {
    return StringUtils.isNotBlank(System.getenv("ABS_ACCOUNT_NAME"))
        && StringUtils.isNotBlank(System.getenv("ABS_ACCOUNT_KEY"))
        && StringUtils.isNotBlank(System.getenv("ABS_CONTAINER_NAME"));
  }
}
