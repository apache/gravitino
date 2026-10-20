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
import org.apache.gravitino.integration.test.container.FixtureContainer;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.HiveFixture;
import org.apache.gravitino.integration.test.container.HiveStorage;
import org.apache.gravitino.integration.test.container.HiveVersion;
import org.apache.gravitino.integration.test.container.LocalStackFixture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@LocalStackFixture
@HiveFixture(version = HiveVersion.HIVE3, storage = HiveStorage.S3)
public class CatalogHiveS3IT extends CatalogHive3IT {

  private static final String S3_BUCKET_NAME = "my-test-bucket";
  @FixtureContainer private GravitinoLocalStackContainer gravitinoLocalStackContainer;

  private String getS3Endpoint;

  @Override
  protected void startNecessaryContainer() {
    hmsCatalog = "hive";

    // The container is ready once its health check passes.
    gravitinoLocalStackContainer.createBucket(S3_BUCKET_NAME);
    gravitinoLocalStackContainer.makeBucketPublic(S3_BUCKET_NAME);

    getS3Endpoint =
        String.format(
            "http://%s:%d",
            gravitinoLocalStackContainer.getContainerIpAddress(),
            GravitinoLocalStackContainer.PORT);

    hiveMetastoreUris =
        String.format(
            "thrift://%s:%d", hive.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
  }

  @Override
  protected void initFileSystem() throws IOException {
    // Use S3a file system
    Configuration conf = new Configuration();
    conf.set("fs.s3a.access.key", GravitinoLocalStackContainer.ACCESS_KEY);
    conf.set("fs.s3a.secret.key", GravitinoLocalStackContainer.SECRET_KEY);
    conf.set("fs.s3a.endpoint", getS3Endpoint);
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3a.connection.ssl.enabled", "false");
    fileSystem = FileSystem.get(URI.create("s3a://" + S3_BUCKET_NAME), conf);
  }

  @Override
  protected void initSparkSession() {
    sparkSession =
        SparkSession.builder()
            .master("local[1]")
            .appName("Hive Catalog integration test")
            .config("hive.metastore.uris", hiveMetastoreUris)
            .config(
                "spark.sql.warehouse.dir",
                String.format(
                    "hdfs://%s:%d/user/hive/warehouse",
                    hive.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT))
            .config("spark.hadoop.fs.s3a.access.key", GravitinoLocalStackContainer.ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", GravitinoLocalStackContainer.SECRET_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", getS3Endpoint)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
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
    properties.put("location", "s3a://" + S3_BUCKET_NAME + "/test-" + System.currentTimeMillis());
    return properties;
  }
}
