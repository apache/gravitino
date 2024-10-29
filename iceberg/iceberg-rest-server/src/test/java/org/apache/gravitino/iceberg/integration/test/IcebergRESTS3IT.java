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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTS3IT extends IcebergRESTJdbcCatalogIT {

  private String s3Warehouse;
  private String accessKey;
  private String secretKey;
  private String region;
  private String roleArn;
  private String externalId;

  @Override
  void initEnv() {
    this.s3Warehouse =
        String.format("s3://%s/test1", getFromEnvOrDefault("GRAVITINO_S3_BUCKET", "{BUCKET_NAME}"));
    this.accessKey = getFromEnvOrDefault("GRAVITINO_S3_ACCESS_KEY", "{ACCESS_KEY}");
    this.secretKey = getFromEnvOrDefault("GRAVITINO_S3_SECRET_KEY", "{SECRET_KEY}");
    this.region = getFromEnvOrDefault("GRAVITINO_S3_REGION", "ap-southeast-2");
    this.roleArn = getFromEnvOrDefault("GRAVITINO_S3_ROLE_ARN", "{ROLE_ARN}");
    this.externalId = getFromEnvOrDefault("GRAVITINO_S3_EXTERNAL_ID", "");
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
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.S3_TOKEN_CREDENTIAL_PROVIDER);
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
    String icebergBundleJarName = "iceberg-aws-bundle-1.5.2.jar";
    String icebergBundleJarUri =
        "https://repo1.maven.org/maven2/org/apache/iceberg/"
            + "iceberg-aws-bundle/1.5.2/"
            + icebergBundleJarName;
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private void copyS3BundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("aws-bundle", targetDir);
  }

  private String getFromEnvOrDefault(String envVar, String defaultValue) {
    String envValue = System.getenv(envVar);
    return Optional.ofNullable(envValue).orElse(defaultValue);
  }
}
