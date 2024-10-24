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
import org.apache.gravitino.credential.config.GCSCredentialConfig;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

// You should export GRAVITINO_GCS_BUCKET and GOOGLE_APPLICATION_CREDENTIALS to run the test
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTGCSIT extends IcebergRESTJdbcCatalogIT {
  private String gcsWarehouse;
  private String gcsCredentialPath;

  @Override
  void initEnv() {
    this.gcsWarehouse =
        String.format("gs://%s/test", getFromEnvOrDefault("GRAVITINO_GCS_BUCKET", "bucketName"));
    this.gcsCredentialPath =
        getFromEnvOrDefault("GOOGLE_APPLICATION_CREDENTIALS", "credential.json");
    if (ITUtils.isEmbedded()) {
      return;
    }

    try {
      downloadIcebergBundleJar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    copyGCSBundleJar();
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    HashMap m = new HashMap<String, String>();
    m.putAll(getCatalogJdbcConfig());
    m.putAll(getGCSConfig());
    return m;
  }

  public boolean supportsCredentialVending() {
    return true;
  }

  private Map<String, String> getGCSConfig() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.GCS_TOKEN_CREDENTIAL_PROVIDER_TYPE);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX
            + GCSCredentialConfig.GRAVITINO_GCS_CREDENTIAL_FILE_PATH,
        gcsCredentialPath);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.gcp.gcs.GCSFileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, gcsWarehouse);
    return configMap;
  }

  private void copyGCSBundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("gcp-bundle", targetDir);
  }

  private void downloadIcebergBundleJar() throws IOException {
    String icebergBundleJarName = "iceberg-gcp-bundle-1.5.2.jar";
    String icebergBundleJarUri =
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/1.5.2/"
            + icebergBundleJarName;
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private String getFromEnvOrDefault(String envVar, String defaultValue) {
    String envValue = System.getenv(envVar);
    return Optional.ofNullable(envValue).orElse(defaultValue);
  }
}
