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
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

@SuppressWarnings("FormatStringAnnotation")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTS3TokenAuthorizationIT extends IcebergRESTCloudTokenAuthorizationBaseIT {

  private String s3Warehouse;
  private String accessKey;
  private String secretKey;
  private String region;
  private String roleArn;
  private String externalId;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    this.s3Warehouse =
        String.format(
            "s3://%s/test1", System.getenv().getOrDefault("GRAVITINO_S3_BUCKET", "{BUCKET_NAME}"));
    this.accessKey = System.getenv().getOrDefault("GRAVITINO_S3_ACCESS_KEY", "{ACCESS_KEY}");
    this.secretKey = System.getenv().getOrDefault("GRAVITINO_S3_SECRET_KEY", "{SECRET_KEY}");
    this.region = System.getenv().getOrDefault("GRAVITINO_S3_REGION", "ap-southeast-2");
    this.roleArn = System.getenv().getOrDefault("GRAVITINO_S3_ROLE_ARN", "{ROLE_ARN}");
    this.externalId = System.getenv().getOrDefault("GRAVITINO_S3_EXTERNAL_ID", "");

    super.startIntegrationTest();

    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());

    setupCloudBundles();
  }

  @Override
  public Map<String, String> getCustomProperties() {
    HashMap<String, String> m = new HashMap<>();
    m.putAll(getS3Config());
    return m;
  }

  @Override
  protected String getCloudProviderName() {
    return "s3";
  }

  @Override
  protected void downloadCloudBundleJar() throws IOException {
    String icebergBundleJarUri =
        String.format(
            "https://repo1.maven.org/maven2/org/apache/iceberg/"
                + "iceberg-aws-bundle/%s/iceberg-aws-bundle-%s.jar",
            ITUtils.icebergVersion(), ITUtils.icebergVersion());
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  @Override
  protected void copyCloudBundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("aws", targetDir);
  }

  private Map<String, String> getS3Config() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);
    configMap.put(S3Properties.GRAVITINO_S3_REGION, region);
    configMap.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    configMap.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretKey);
    configMap.put(S3Properties.GRAVITINO_S3_ROLE_ARN, roleArn);
    if (StringUtils.isNotBlank(externalId)) {
      configMap.put(S3Properties.GRAVITINO_S3_EXTERNAL_ID, externalId);
    }

    configMap.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    configMap.put(IcebergConstants.WAREHOUSE, s3Warehouse);

    return configMap;
  }
}
