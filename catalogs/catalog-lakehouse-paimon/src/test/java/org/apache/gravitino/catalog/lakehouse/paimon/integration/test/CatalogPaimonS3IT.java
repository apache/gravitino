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

package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.Container;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@Tag("gravitino-docker-test")
public class CatalogPaimonS3IT extends CatalogPaimonBaseIT {

  private static final String S3_BUCKET_NAME = "my-test-bucket";
  private static GravitinoLocalStackContainer localStackContainer;
  private String accessKey;
  private String secretKey;
  private String endpoint;

  private static final String PAIMON_S3_JAR_URL =
      "https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/0.8.0/paimon-s3-0.8.0.jar";

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "filesystem";
    WAREHOUSE = "s3://" + S3_BUCKET_NAME + "/";

    accessKey = "accessKey";
    secretKey = "secretKey";
    endpoint = String.format("http://%s:%d", localStackContainer.getContainerIpAddress(), 4566);

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    catalogProperties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretKey);
    catalogProperties.put(S3Properties.GRAVITINO_S3_ENDPOINT, endpoint);

    // Need to download the S3 dependency in the deploy mode.
    downloadS3Dependency();

    return catalogProperties;
  }

  private void downloadS3Dependency() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    try {
      if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
        String paimonCatalogPath =
            ITUtils.joinPath(gravitinoHome, "catalogs", "lakehouse-paimon", "libs");
        DownloaderUtils.downloadFile(PAIMON_S3_JAR_URL, paimonCatalogPath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to download the S3 dependency", e);
    }
  }

  @Override
  protected void startNecessaryContainers() {
    containerSuite.startLocalStackContainer();
    localStackContainer = containerSuite.getLocalStackContainer();

    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                Container.ExecResult result =
                    localStackContainer.executeInContainer(
                        "awslocal", "s3", "mb", "s3://" + S3_BUCKET_NAME);
                return result.getExitCode() == 0;
              } catch (Exception e) {
                return false;
              }
            });
  }

  protected void initSparkEnv() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("Paimon Catalog integration test")
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
            .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE)
            .config(
                "spark.sql.extensions",
                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
            .config("spark.sql.catalog.paimon.s3.access-key", accessKey)
            .config("spark.sql.catalog.paimon.s3.secret-key", secretKey)
            .config("spark.sql.catalog.paimon.s3.endpoint", endpoint)
            .enableHiveSupport()
            .getOrCreate();
  }
}
