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
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled(
    "You need to specify the real OSS bucket name, access key, secret key and endpoint to run this test")
public class CatalogPaimonOSSIT extends CatalogPaimonBaseIT {

  private static final String OSS_BUCKET_NAME = "YOUR_BUCKET";
  private static final String accessKey = "YOUR_ACCESS_KEY";
  private static final String secretKey = "YOUR_SECRET_KEY";
  private static final String endpoint = "OSS_ENDPOINT";
  private static final String warehouse = "oss://" + OSS_BUCKET_NAME + "/paimon-test";

  private static final String PAIMON_OSS_JAR_URL =
      "https://repo1.maven.org/maven2/org/apache/paimon/paimon-oss/0.8.0/paimon-oss-0.8.0.jar";

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "filesystem";

    WAREHOUSE = warehouse;
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, accessKey);
    catalogProperties.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, secretKey);
    catalogProperties.put(OSSProperties.GRAVITINO_OSS_ENDPOINT, endpoint);

    // Need to download the OSS dependency in the deploy mode.
    downloadOSSDependency();

    return catalogProperties;
  }

  private void downloadOSSDependency() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    try {
      if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
        String paimonCatalogPath =
            ITUtils.joinPath(gravitinoHome, "catalogs", "lakehouse-paimon", "libs");
        DownloaderUtils.downloadFile(PAIMON_OSS_JAR_URL, paimonCatalogPath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to download the OSS dependency", e);
    }
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
            .config("spark.sql.catalog.paimon.fs.oss.accessKeyId", accessKey)
            .config("spark.sql.catalog.paimon.fs.oss.accessKeySecret", secretKey)
            .config("spark.sql.catalog.paimon.fs.oss.endpoint", endpoint)
            .enableHiveSupport()
            .getOrCreate();
  }
}
