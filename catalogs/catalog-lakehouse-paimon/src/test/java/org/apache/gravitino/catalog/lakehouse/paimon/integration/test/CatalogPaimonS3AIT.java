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

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.GravitinoLocalStackContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

/** Integration test for accessing a Paimon filesystem catalog through Hadoop S3A. */
@Tag("gravitino-docker-test")
public class CatalogPaimonS3AIT extends BaseIT {

  private static final String PROVIDER = "lakehouse-paimon";
  private static final String S3A_PROPERTY_PREFIX = CATALOG_BYPASS_PREFIX + "hadoop.fs.s3a.";
  private static final String ACCESS_KEY = "test";
  private static final String SECRET_KEY = "test";

  private final String bucketName = "paimon-s3a-" + UUID.randomUUID().toString().replace("-", "");
  private final String metalakeName = GravitinoITUtils.genRandomName("paimon_s3a_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("paimon_s3a_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("paimon_s3a_schema");

  private GravitinoMetalake metalake;
  private GravitinoLocalStackContainer localStackContainer;

  @BeforeAll
  void setUp() {
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
                        "awslocal", "s3", "mb", "s3://" + bucketName);
                return result.getExitCode() == 0;
              } catch (Exception e) {
                return false;
              }
            });

    metalake = client.createMetalake(metalakeName, "Paimon S3A metalake", Collections.emptyMap());
  }

  @AfterAll
  void tearDown() {
    if (metalake != null && metalake.catalogExists(catalogName)) {
      metalake.disableCatalog(catalogName);
      metalake.dropCatalog(catalogName, true);
    }
    if (client != null && client.metalakeExists(metalakeName)) {
      client.disableMetalake(metalakeName);
      client.dropMetalake(metalakeName, true);
    }
  }

  @Test
  void testAccessS3AFileSystem() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    catalogProperties.put(
        PaimonCatalogPropertiesMetadata.WAREHOUSE, "s3a://" + bucketName + "/warehouse");
    catalogProperties.put(S3A_PROPERTY_PREFIX + "access.key", ACCESS_KEY);
    catalogProperties.put(S3A_PROPERTY_PREFIX + "secret.key", SECRET_KEY);
    catalogProperties.put(
        S3A_PROPERTY_PREFIX + "endpoint",
        String.format(
            "http://localhost:%d",
            localStackContainer.getMappedPort(GravitinoLocalStackContainer.PORT)));
    catalogProperties.put(S3A_PROPERTY_PREFIX + "impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    catalogProperties.put(
        S3A_PROPERTY_PREFIX + "aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    catalogProperties.put(S3A_PROPERTY_PREFIX + "path.style.access", "true");
    catalogProperties.put(S3A_PROPERTY_PREFIX + "connection.ssl.enabled", "false");

    Catalog catalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            PROVIDER,
            "Paimon S3A catalog",
            catalogProperties);
    Schema schema =
        catalog.asSchemas().createSchema(schemaName, "Paimon S3A schema", Collections.emptyMap());

    Assertions.assertEquals(schemaName, schema.name());
    Assertions.assertEquals(schemaName, catalog.asSchemas().loadSchema(schemaName).name());
  }
}
