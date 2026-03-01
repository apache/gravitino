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
package org.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Iceberg catalog with JDBC backend credential vending. Tests that JDBC
 * credentials can be retrieved through the credential API.
 */
@Tag("gravitino-docker-test")
public class CatalogIcebergJdbcCredentialIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogIcebergJdbcCredentialIT.class);

  private static final String JDBC_USER = "iceberg_user";
  private static final String JDBC_PASSWORD = "iceberg_password";

  private String metalakeName = GravitinoITUtils.genRandomName("iceberg_jdbc_credential_metalake");
  private String catalogName = GravitinoITUtils.genRandomName("iceberg_jdbc_credential_catalog");
  private GravitinoMetalake metalake;

  @BeforeAll
  public void startIntegrationTest() {
    // Do nothing - override to prevent auto start
  }

  @BeforeAll
  public void startUp() throws Exception {
    super.ignoreIcebergAuxRestService = false;
    super.startIntegrationTest();

    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(client.metalakeExists(metalakeName));

    createCatalog();
  }

  @AfterAll
  public void tearDown() throws IOException {
    try {
      if (metalake != null && metalake.catalogExists(catalogName)) {
        metalake.disableCatalog(catalogName);
        metalake.dropCatalog(catalogName, true);
      }
      if (client != null && client.metalakeExists(metalakeName)) {
        client.disableMetalake(metalakeName);
        client.dropMetalake(metalakeName, true);
      }
    } finally {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          LOG.error("Exception in closing client", e);
        }
        client = null;
      }
      try {
        super.stopIntegrationTest();
      } catch (Exception e) {
        LOG.error("Exception in closing CloseableGroup", e);
      }
    }
  }

  @AfterAll
  public void stopIntegrationTest() {
    // Do nothing - override to prevent auto stop
  }

  private void createCatalog() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(IcebergConfig.CATALOG_BACKEND.getKey(), "jdbc");
    catalogProperties.put(IcebergConfig.CATALOG_URI.getKey(), "jdbc:sqlite::memory:");
    catalogProperties.put(IcebergConfig.CATALOG_WAREHOUSE.getKey(), "/tmp/iceberg-jdbc-test");
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.sqlite.JDBC");
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_USER, JDBC_USER);
    catalogProperties.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, JDBC_PASSWORD);

    Catalog createdCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "JDBC backend Iceberg catalog for credential testing",
            catalogProperties);
    Assertions.assertNotNull(createdCatalog);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
  }

  @Test
  void testGetJdbcCredentialFromCatalog() {
    Catalog catalog = metalake.loadCatalog(catalogName);
    Credential[] credentials = catalog.supportsCredentials().getCredentials();

    // Should have JDBC credential automatically configured for JDBC backend
    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(JdbcCredential.class, credentials[0]);

    JdbcCredential jdbcCredential = (JdbcCredential) credentials[0];
    Assertions.assertEquals(JDBC_USER, jdbcCredential.jdbcUser());
    Assertions.assertEquals(JDBC_PASSWORD, jdbcCredential.jdbcPassword());
    Assertions.assertEquals(0, jdbcCredential.expireTimeInMs());
    Assertions.assertEquals(JdbcCredential.JDBC_CREDENTIAL_TYPE, jdbcCredential.credentialType());
  }

  @Test
  void testGetJdbcCredentialByType() {
    Catalog catalog = metalake.loadCatalog(catalogName);
    Credential credential =
        catalog.supportsCredentials().getCredential(JdbcCredential.JDBC_CREDENTIAL_TYPE);

    Assertions.assertNotNull(credential);
    Assertions.assertInstanceOf(JdbcCredential.class, credential);

    JdbcCredential jdbcCredential = (JdbcCredential) credential;
    Assertions.assertEquals(JDBC_USER, jdbcCredential.jdbcUser());
    Assertions.assertEquals(JDBC_PASSWORD, jdbcCredential.jdbcPassword());
  }
}
