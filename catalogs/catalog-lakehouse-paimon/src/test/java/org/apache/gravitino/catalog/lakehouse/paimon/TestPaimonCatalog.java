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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.CATALOG_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.SCHEMA_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalog.TABLE_PROPERTIES_META;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalog {

  static final HasPropertyMetadata PAIMON_PROPERTIES_METADATA =
      new HasPropertyMetadata() {

        @Override
        public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
          return TABLE_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
          return CATALOG_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
          return SCHEMA_PROPERTIES_META;
        }

        @Override
        public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Fileset properties are not supported");
        }

        @Override
        public PropertiesMetadata topicPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Topic properties are not supported");
        }

        @Override
        public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Model properties are not supported");
        }

        @Override
        public PropertiesMetadata modelVersionPropertiesMetadata()
            throws UnsupportedOperationException {
          throw new UnsupportedOperationException("Does not support model version properties");
        }
      };

  private static String tempDir =
      String.join(File.separator, System.getProperty("java.io.tmpdir"), "paimon_catalog_warehouse");

  @AfterAll
  public static void clean() {
    try {
      FileUtils.deleteDirectory(new File(tempDir));
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testCatalogOperation() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, tempDir);
    PaimonCatalog paimonCatalog =
        new PaimonCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    CatalogOperations catalogOperations = paimonCatalog.ops();
    Assertions.assertInstanceOf(PaimonCatalogOperations.class, catalogOperations);

    PaimonCatalogOperations paimonCatalogOperations = (PaimonCatalogOperations) catalogOperations;
    PaimonCatalogOps paimonCatalogOps = paimonCatalogOperations.paimonCatalogOps;
    Assertions.assertEquals(
        paimonCatalogOperations.listSchemas(Namespace.empty()).length,
        paimonCatalogOps.listDatabases().size());

    // test testConnection
    Assertions.assertDoesNotThrow(
        () ->
            paimonCatalogOperations.testConnection(
                NameIdentifier.of("metalake", "catalog"),
                Catalog.Type.RELATIONAL,
                "paimon",
                "comment",
                ImmutableMap.of()));
  }

  @Test
  void testCatalogProperty() throws IOException {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    conf.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    conf.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, tempDir);
    try (PaimonCatalogOperations ops = new PaimonCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), PAIMON_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = PAIMON_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1));

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
      map2.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, "test");
      map2.put(PaimonCatalogPropertiesMetadata.URI, "127.0.0.1");
      Assertions.assertDoesNotThrow(
          () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2));

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable
              .getMessage()
              .contains(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND));
    }
  }

  @Test
  void testJdbcBackendDefaultCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC backend with jdbc-user and jdbc-password
    Map<String, String> jdbcProps = Maps.newHashMap();
    jdbcProps.put(PaimonConstants.CATALOG_BACKEND, "jdbc");
    jdbcProps.put(PaimonConstants.URI, "jdbc:sqlite::memory:");
    jdbcProps.put(PaimonConstants.WAREHOUSE, tempDir);
    jdbcProps.put(PaimonConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcProps.put(PaimonConstants.GRAVITINO_JDBC_PASSWORD, "test-password");

    CatalogEntity jdbcEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("jdbc-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcProps)
            .build();

    PaimonCatalog jdbcCatalog =
        new PaimonCatalog().withCatalogConf(jdbcProps).withCatalogEntity(jdbcEntity);
    Map<String, String> properties = jdbcCatalog.properties();

    // Should have jdbc credential provider
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNotNull(credentialProviders);
    Assertions.assertTrue(credentialProviders.contains(JdbcCredential.JDBC_CREDENTIAL_TYPE));
  }

  @Test
  void testJdbcBackendWithS3CredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC backend with jdbc-user, jdbc-password, and S3 credentials
    Map<String, String> jdbcS3Props = Maps.newHashMap();
    jdbcS3Props.put(PaimonConstants.CATALOG_BACKEND, "jdbc");
    jdbcS3Props.put(PaimonConstants.URI, "jdbc:sqlite::memory:");
    jdbcS3Props.put(PaimonConstants.WAREHOUSE, tempDir);
    jdbcS3Props.put(PaimonConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcS3Props.put(PaimonConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    jdbcS3Props.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "access-key");
    jdbcS3Props.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "secret-key");

    CatalogEntity jdbcS3Entity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("jdbc-s3-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcS3Props)
            .build();

    PaimonCatalog jdbcS3Catalog =
        new PaimonCatalog().withCatalogConf(jdbcS3Props).withCatalogEntity(jdbcS3Entity);
    Map<String, String> properties = jdbcS3Catalog.properties();

    // Should have both jdbc and s3-secret-key credential providers
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNotNull(credentialProviders);
    Assertions.assertTrue(credentialProviders.contains(JdbcCredential.JDBC_CREDENTIAL_TYPE));
    Assertions.assertTrue(
        credentialProviders.contains(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
  }

  @Test
  void testNonJdbcBackendNoDefaultCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test non-JDBC backend (filesystem) - should not add default credential providers
    Map<String, String> fsProps = Maps.newHashMap();
    fsProps.put(PaimonConstants.CATALOG_BACKEND, "filesystem");
    fsProps.put(PaimonConstants.WAREHOUSE, tempDir);

    CatalogEntity fsEntity =
        CatalogEntity.builder()
            .withId(3L)
            .withName("fs-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .withProperties(fsProps)
            .build();

    PaimonCatalog fsCatalog =
        new PaimonCatalog().withCatalogConf(fsProps).withCatalogEntity(fsEntity);
    Map<String, String> properties = fsCatalog.properties();

    // Should not have credential providers for non-JDBC backend
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNull(credentialProviders);
  }

  @Test
  void testExplicitCredentialProvidersNotOverridden() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test that explicit credential-providers setting is not overridden
    Map<String, String> explicitProps = Maps.newHashMap();
    explicitProps.put(PaimonConstants.CATALOG_BACKEND, "jdbc");
    explicitProps.put(PaimonConstants.URI, "jdbc:sqlite::memory:");
    explicitProps.put(PaimonConstants.WAREHOUSE, tempDir);
    explicitProps.put(PaimonConstants.GRAVITINO_JDBC_USER, "test-user");
    explicitProps.put(PaimonConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    explicitProps.put(CredentialConstants.CREDENTIAL_PROVIDERS, "custom-provider");

    CatalogEntity explicitEntity =
        CatalogEntity.builder()
            .withId(4L)
            .withName("explicit-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(PaimonCatalog.Type.RELATIONAL)
            .withProvider("lakehouse-paimon")
            .withAuditInfo(auditInfo)
            .withProperties(explicitProps)
            .build();

    PaimonCatalog explicitCatalog =
        new PaimonCatalog().withCatalogConf(explicitProps).withCatalogEntity(explicitEntity);
    Map<String, String> properties = explicitCatalog.properties();

    // Should keep explicit credential providers, not override
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertEquals("custom-provider", credentialProviders);
  }
}
