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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.GCSProperties;
import org.junit.jupiter.api.Disabled;

// GOOGLE_APPLICATION_CREDENTIALS=/Users/fanng/deploy/gcs/tonal-land-426304-d3-a75b6878b6ce.json
@Disabled(
    "You should export GRAVITINO_GCS_BUCKET and GOOGLE_APPLICATION_CREDENTIALS to run the test")
public class IcebergRESTGCSIT extends IcebergRESTJdbcCatalogIT {
  private String gcsWarehouse;
  private String gcsCredentialPath;

  @Override
  void initEnv() {
    this.gcsWarehouse =
        String.format("gs://%s/test", getFromEnvOrDefault("GRAVITINO_GCS_BUCKET", "bucketName"));
    this.gcsCredentialPath =
        getFromEnvOrDefault("GOOGLE_APPLICATION_CREDENTIALS", "credentialPath");
    if (ITUtils.isEmbedded()) {
      return;
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
    return false;
  }

  private Map<String, String> getGCSConfig() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.GCS_TOKEN_CREDENTIAL_TYPE);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + GCSProperties.GRAVITINO_GCS_CREDENTIAL_FILE_PATH,
        gcsCredentialPath);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.gcp.gcs.GCSFileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, gcsWarehouse);
    return configMap;
  }

  private void copyGCSBundleJar() {
    String gravitinoVersion = System.getenv("PROJECT_VERSION");
    String gcsBundleFile = String.format("gravitino-gcp-bundle-%s.jar", gravitinoVersion);

    String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
    String sourceFile =
        String.format("%s/bundles/gcp-bundle/build/libs/%s", rootDir, gcsBundleFile);
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    String targetFile = String.format("%s/%s", targetDir, gcsBundleFile);
    LOG.info("Source file: {}, target directory: {}", sourceFile, targetDir);
    try {
      File target = new File(targetFile);
      if (!target.exists()) {
        LOG.info("Copy source file: {} to target directory: {}", sourceFile, targetDir);
        FileUtils.copyFileToDirectory(new File(sourceFile), new File(targetDir));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFromEnvOrDefault(String envVar, String defaultValue) {
    String envValue = System.getenv(envVar);
    return Optional.ofNullable(envValue).orElse(defaultValue);
  }
}
