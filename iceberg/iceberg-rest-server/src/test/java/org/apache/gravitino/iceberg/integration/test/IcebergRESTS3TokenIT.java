/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.iceberg.TableProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

@SuppressWarnings("FormatStringAnnotation")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTS3TokenIT extends IcebergRESTJdbcCatalogIT {

  private String s3Warehouse;
  private String accessKey;
  private String secretKey;
  private String region;
  private String roleArn;
  private String externalId;

  @Override
  void initEnv() {
    this.s3Warehouse =
        String.format(
            "s3://%s/test1", System.getenv().getOrDefault("GRAVITINO_S3_BUCKET", "{BUCKET_NAME}"));
    this.accessKey = System.getenv().getOrDefault("GRAVITINO_S3_ACCESS_KEY", "{ACCESS_KEY}");
    this.secretKey = System.getenv().getOrDefault("GRAVITINO_S3_SECRET_KEY", "{SECRET_KEY}");
    this.region = System.getenv().getOrDefault("GRAVITINO_S3_REGION", "ap-southeast-2");
    this.roleArn = System.getenv().getOrDefault("GRAVITINO_S3_ROLE_ARN", "{ROLE_ARN}");
    this.externalId = System.getenv().getOrDefault("GRAVITINO_S3_EXTERNAL_ID", "");
    if (ITUtils.isEmbedded()) {
      return;
    }
    try {
      downloadIcebergAwsBundleJar();
    } catch (IOException e) {
      LOG.warn("Download Iceberg AWS bundle jar failed,", e);
      throw new RuntimeException(e);
    }
    copyS3BundleJar();
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    HashMap m = new HashMap<String, String>();
    m.putAll(getCatalogJdbcConfig());
    m.putAll(getS3Config());
    return m;
  }

  public boolean supportsCredentialVending() {
    return true;
  }

  private Map<String, String> getS3Config() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDERS,
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + S3Properties.GRAVITINO_S3_REGION, region);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
        secretKey);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + S3Properties.GRAVITINO_S3_ROLE_ARN, roleArn);
    if (StringUtils.isNotBlank(externalId)) {
      configMap.put(
          IcebergConfig.ICEBERG_CONFIG_PREFIX + S3Properties.GRAVITINO_S3_EXTERNAL_ID, externalId);
    }

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.aws.s3.S3FileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, s3Warehouse);

    return configMap;
  }

  private void downloadIcebergAwsBundleJar() throws IOException {
    String icebergBundleJarUri =
        String.format(
            "https://repo1.maven.org/maven2/org/apache/iceberg/"
                + "iceberg-aws-bundle/%s/iceberg-aws-bundle-%s.jar",
            ITUtils.icebergVersion(), ITUtils.icebergVersion());
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private void copyS3BundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("aws", targetDir);
  }

  /**
   * Parses a string representing table properties into a map of key-value pairs.
   *
   * @param tableProperties A string representing the table properties in the format:
   *     "[key1=value1,key2=value2,...]"
   * @return A Map where each key is a property name (String) and the corresponding value is the
   *     property value (String). Example input:
   *     "[write.data.path=path/to/data,write.metadata.path=path/to/metadata]" Example output: {
   *     "write.data.path" -> "path/to/data", "write.metadata.path" -> "path/to/metadata" }
   */
  private Map<String, String> parseTableProperties(String tableProperties) {
    Map<String, String> propertiesMap = new HashMap<>();
    String[] pairs = tableProperties.substring(1, tableProperties.length() - 1).split(",");
    for (String pair : pairs) {
      String[] keyValue = pair.split("=", 2); // Split at most once
      if (keyValue.length == 2) {
        propertiesMap.put(keyValue[0].trim(), keyValue[1].trim());
      }
    }
    return propertiesMap;
  }

  @Test
  void testCredentialWithMultiLocations() {
    String namespaceName = ICEBERG_REST_NS_PREFIX + "credential";
    String tableName = namespaceName + ".multi_location";

    String writeDataPath = this.s3Warehouse + "/test_data_location";
    String writeMetaDataPath = this.s3Warehouse + "/test_metadata_location";

    sql("CREATE DATABASE IF NOT EXISTS " + namespaceName);
    sql(
        String.format(
            "CREATE TABLE %s (id bigint) USING iceberg OPTIONS ('%s' = '%s', '%s' = '%s')",
            tableName,
            TableProperties.WRITE_DATA_LOCATION,
            writeDataPath,
            TableProperties.WRITE_METADATA_LOCATION,
            writeMetaDataPath));

    Map<String, String> tableDetails =
        convertToStringMap(sql("DESCRIBE TABLE EXTENDED " + tableName));
    String tableProperties = tableDetails.get("Table Properties");
    Assertions.assertNotNull(tableProperties, "Table Properties should not be null");

    Map<String, String> propertiesMap = parseTableProperties(tableProperties);
    Assertions.assertEquals(
        writeDataPath,
        propertiesMap.get(TableProperties.WRITE_DATA_LOCATION),
        String.format(
            "Expected write.data.path to be '%s', but was '%s'",
            writeDataPath, propertiesMap.get(TableProperties.WRITE_DATA_LOCATION)));
    Assertions.assertEquals(
        writeMetaDataPath,
        propertiesMap.get(TableProperties.WRITE_METADATA_LOCATION),
        String.format(
            "Expected write.metadata.path to be '%s', but was '%s'",
            writeMetaDataPath, propertiesMap.get(TableProperties.WRITE_METADATA_LOCATION)));

    String value1 = "1";
    String value2 = "2";
    sql(String.format("INSERT INTO %s VALUES (%s), (%s);", tableName, value1, value2));
    List<String> result = convertToStringList(sql(String.format("SELECT * FROM %s", tableName)), 0);
    Assertions.assertEquals(result, ImmutableList.of((value1), (value2)));
  }
}
