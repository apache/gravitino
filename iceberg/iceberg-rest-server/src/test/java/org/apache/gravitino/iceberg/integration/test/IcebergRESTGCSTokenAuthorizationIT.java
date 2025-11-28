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
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.GCSProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests for Iceberg REST credential vending with GCS (Google Cloud Storage).
 *
 * <p>Authentication: Supports both service account key file and Application Default Credentials
 * (ADC).
 *
 * <p>Required environment variables:
 *
 * <ul>
 *   <li>GRAVITINO_TEST_CLOUD_IT=true - enables cloud integration tests
 *   <li>GRAVITINO_GCS_BUCKET - GCS bucket name (e.g., "my-bucket")
 *   <li>GRAVITINO_GCS_PATH_PREFIX - path prefix within bucket (e.g., "test/gravitino")
 *   <li>GRAVITINO_GCS_SERVICE_ACCOUNT_FILE (optional) - path to service account JSON key. If
 *       omitted, uses ADC (requires: gcloud auth application-default login)
 * </ul>
 */
@SuppressWarnings("FormatStringAnnotation")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTGCSTokenAuthorizationIT extends IcebergRESTCloudTokenAuthorizationBaseIT {

  private String gcsWarehouse;
  private String serviceAccountFile;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    String bucket = System.getenv().getOrDefault("GRAVITINO_GCS_BUCKET", "{BUCKET_NAME}");
    String pathPrefix = System.getenv().getOrDefault("GRAVITINO_GCS_PATH_PREFIX", "test1");
    this.gcsWarehouse = String.format("gs://%s/%s", bucket, pathPrefix);
    // Use null to trigger ADC (Application Default Credentials) if not explicitly provided
    this.serviceAccountFile = System.getenv().get("GRAVITINO_GCS_SERVICE_ACCOUNT_FILE");

    super.startIntegrationTest();

    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());

    setupCloudBundles();
  }

  @Override
  public Map<String, String> getCustomProperties() {
    HashMap<String, String> m = new HashMap<>();
    m.putAll(getGCSConfig());
    return m;
  }

  @Override
  protected String getCloudProviderName() {
    return "gcs";
  }

  @Override
  protected void downloadCloudBundleJar() throws IOException {
    String icebergBundleJarUri =
        String.format(
            "https://repo1.maven.org/maven2/org/apache/iceberg/"
                + "iceberg-gcp-bundle/%s/iceberg-gcp-bundle-%s.jar",
            ITUtils.icebergVersion(), ITUtils.icebergVersion());
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  @Override
  protected void copyCloudBundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("gcp", targetDir);
  }

  private Map<String, String> getGCSConfig() {
    Map<String, String> configMap = new HashMap<>();

    configMap.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE);
    // Only set service account file if explicitly provided, otherwise use ADC
    if (serviceAccountFile != null && !serviceAccountFile.isEmpty()) {
      configMap.put(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, serviceAccountFile);
    }

    configMap.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
    configMap.put(IcebergConstants.WAREHOUSE, gcsWarehouse);

    return configMap;
  }
}
