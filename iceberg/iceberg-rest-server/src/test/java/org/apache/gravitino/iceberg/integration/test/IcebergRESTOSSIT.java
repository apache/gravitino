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
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.OSSProperties;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTOSSIT extends IcebergRESTJdbcCatalogIT {

  private String warehouse;
  private String accessKey;
  private String secretKey;
  private String endpoint;
  private String roleArn;
  private String externalId;
  private String region;

  @Override
  void initEnv() {
    this.warehouse =
        String.format(
            "oss://%s/gravitino-test",
            System.getenv().getOrDefault("GRAVITINO_OSS_BUCKET", "{BUCKET_NAME}"));
    this.accessKey = System.getenv().getOrDefault("GRAVITINO_OSS_ACCESS_KEY", "{ACCESS_KEY}");
    this.secretKey = System.getenv().getOrDefault("GRAVITINO_OSS_SECRET_KEY", "{SECRET_KEY}");
    this.endpoint =
        System.getenv().getOrDefault("GRAVITINO_OSS_ENDPOINT", "{GRAVITINO_OSS_ENDPOINT}");
    this.region = System.getenv().getOrDefault("GRAVITINO_OSS_REGION", "oss-cn-hangzhou");
    this.roleArn = System.getenv().getOrDefault("GRAVITINO_OSS_ROLE_ARN", "{ROLE_ARN}");
    this.externalId = System.getenv().getOrDefault("GRAVITINO_OSS_EXTERNAL_ID", "");

    if (ITUtils.isEmbedded()) {
      return;
    }
    try {
      downloadIcebergForAliyunJar();
    } catch (IOException e) {
      LOG.warn("Download Iceberg Aliyun bundle jar failed,", e);
      throw new RuntimeException(e);
    }
    copyAliyunOSSJar();
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    HashMap m = new HashMap<String, String>();
    m.putAll(getCatalogJdbcConfig());
    m.putAll(getOSSConfig());
    return m;
  }

  public boolean supportsCredentialVending() {
    return true;
  }

  private Map<String, String> getOSSConfig() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.OSS_TOKEN_CREDENTIAL_PROVIDER);
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_REGION, region);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_ENDPOINT, endpoint);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, accessKey);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
        secretKey);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_ROLE_ARN, roleArn);
    if (StringUtils.isNotBlank(externalId)) {
      configMap.put(
          IcebergConfig.ICEBERG_CONFIG_PREFIX + OSSProperties.GRAVITINO_OSS_EXTERNAL_ID,
          externalId);
    }

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.aliyun.oss.OSSFileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, warehouse);

    return configMap;
  }

  private void downloadIcebergForAliyunJar() throws IOException {
    String icebergBundleJarName = "iceberg-aliyun-1.5.2.jar";
    String icebergBundleJarUri =
        "https://repo1.maven.org/maven2/org/apache/iceberg/"
            + "iceberg-aliyun/1.5.2/"
            + icebergBundleJarName;
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private void copyAliyunOSSJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("aliyun-bundle", targetDir);
  }
}
