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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.MountableFile;

@EnabledIf(value = "isGCSConfigured", disabledReason = "Google Cloud Storage(GCS) is not prepared.")
public class CatalogHiveGCSIT extends CatalogHiveIT {

  private static final String GCS_BUCKET_NAME = System.getenv("GCS_BUCKET_NAME");
  private static final String GCS_ACCOUNT_JSON_FILE =
      System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH");
  private static final String GCS_ACCOUNT_JSON_FILE_IN_CONTAINER = "/tmp/gcs-service-account.json";

  @Override
  protected void startNecessaryContainer() {
    Map<String, String> hiveContainerEnv =
        ImmutableMap.of(
            "SERVICE_ACCOUNT_FILE",
            GCS_ACCOUNT_JSON_FILE_IN_CONTAINER,
            HiveContainer.HIVE_RUNTIME_VERSION,
            HiveContainer.HIVE3);

    containerSuite.startHiveContainerWithS3(hiveContainerEnv);

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainerWithS3().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    containerSuite
        .getHiveContainerWithS3()
        .getContainer()
        .copyFileToContainer(
            MountableFile.forHostPath(GCS_ACCOUNT_JSON_FILE), "/tmp/gcs-service-account.json");
  }

  @Override
  protected void initFileSystem() throws IOException {
    Configuration conf = new Configuration();

    conf.set("fs.gs.auth.service.account.enable", "true");
    conf.set("fs.gs.auth.service.account.json.keyfile", GCS_ACCOUNT_JSON_FILE);

    String path = String.format("gs://%s/", GCS_BUCKET_NAME);
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
                String.format(String.format("gs://%s/user/hive/warehouse", GCS_BUCKET_NAME)))
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", GCS_ACCOUNT_JSON_FILE)
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
        "location", String.format("gs://%s/test-%s", GCS_BUCKET_NAME, System.currentTimeMillis()));
    return properties;
  }

  private static boolean isGCSConfigured() {
    return StringUtils.isNotBlank(System.getenv("GCS_BUCKET_NAME"))
        && StringUtils.isNotBlank(System.getenv("GCS_SERVICE_ACCOUNT_JSON_PATH"));
  }
}
