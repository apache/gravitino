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

package org.apache.gravitino.catalog.fileset.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;
import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.LOCATION;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.container.MinIOContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Tests an existing Fileset catalog connection with S3 temporary credentials against MinIO. */
@Tag("gravitino-docker-test")
public class FilesetS3TokenConnectionIT extends BaseIT {

  private static final String ROLE_ARN = "arn:minio:iam:::role/test";
  private static final String REGION = "us-east-1";

  private final String bucketName =
      "fileset-connection-" + UUID.randomUUID().toString().replace("-", "");
  private final String metalakeName = GravitinoITUtils.genRandomName("fileset_connection_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("fileset_connection_catalog");

  private GravitinoMetalake metalake;

  /** {@inheritDoc} */
  @Override
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");
    containerSuite.startMinIOContainer();
    MinIOContainer minIOContainer = containerSuite.getMinIOContainer();
    minIOContainer.createBucket(bucketName);

    super.startIntegrationTest();

    client.createMetalake(metalakeName, "comment", new HashMap<>());
    metalake = client.loadMetalake(metalakeName);

    String endpoint = minIOContainer.getS3Endpoint();
    Map<String, String> properties = new HashMap<>();
    properties.put(LOCATION, String.format("s3a://%s", bucketName));
    properties.put(FILESYSTEM_PROVIDERS, "s3");
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);
    properties.put(S3Properties.GRAVITINO_S3_REGION, REGION);
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, MinIOContainer.ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, MinIOContainer.SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, ROLE_ARN);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, endpoint);
    properties.put(S3Properties.GRAVITINO_S3_STS_ENDPOINT, endpoint);
    properties.put(S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, "true");

    metalake.createCatalog(catalogName, Catalog.Type.FILESET, "hadoop", "comment", properties);
  }

  @AfterAll
  void cleanup() {
    if (metalake != null) {
      client.dropMetalake(metalakeName, true);
    }
  }

  @Test
  void testExistingCatalogConnectionWithS3Token() throws Exception {
    metalake.testConnection(catalogName);
  }
}
