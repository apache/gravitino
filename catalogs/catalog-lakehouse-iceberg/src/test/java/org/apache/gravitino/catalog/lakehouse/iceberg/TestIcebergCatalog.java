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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.CATALOG_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.SCHEMA_PROPERTIES_META;
import static org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalog.TABLE_PROPERTIES_META;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalog {
  static final HasPropertyMetadata ICEBERG_PROPERTIES_METADATA =
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

  @Test
  public void testListDatabases() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);
    CatalogOperations catalogOperations = icebergCatalog.ops();
    Assertions.assertTrue(catalogOperations instanceof IcebergCatalogOperations);

    IcebergCatalogWrapper icebergCatalogWrapper =
        ((IcebergCatalogOperations) catalogOperations).icebergCatalogWrapper;
    ListNamespacesResponse listNamespacesResponse =
        icebergCatalogWrapper.listNamespace(org.apache.iceberg.catalog.Namespace.empty());
    Assertions.assertTrue(listNamespacesResponse.namespaces().isEmpty());
  }

  @Test
  void testCatalogProperty() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = ICEBERG_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1);
          });

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "hive");
      map2.put(IcebergCatalogPropertiesMetadata.URI, "127.0.0.1");
      map2.put(IcebergCatalogPropertiesMetadata.WAREHOUSE, "test");
      Assertions.assertDoesNotThrow(
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2);
          });

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable.getMessage().contains(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND));
    }
  }

  @Test
  void testCatalogInstanciation() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();

    try (IcebergCatalogOperations ops = new IcebergCatalogOperations()) {
      ops.initialize(conf, entity.toCatalogInfo(), ICEBERG_PROPERTIES_METADATA);
      Map<String, String> map1 = Maps.newHashMap();
      map1.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "test");
      PropertiesMetadata metadata = ICEBERG_PROPERTIES_METADATA.catalogPropertiesMetadata();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map1);
          });

      Map<String, String> map2 = Maps.newHashMap();
      map2.put(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND, "rest");
      map2.put(IcebergCatalogPropertiesMetadata.URI, "127.0.0.1");
      Assertions.assertDoesNotThrow(
          () -> {
            PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map2);
          });

      Map<String, String> map3 = Maps.newHashMap();
      Throwable throwable =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PropertiesMetadataHelpers.validatePropertyForCreate(metadata, map3));

      Assertions.assertTrue(
          throwable.getMessage().contains(IcebergCatalogPropertiesMetadata.CATALOG_BACKEND));
    }
  }

  @Test
  public void testAsViewCatalog() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .build();

    Map<String, String> conf = Maps.newHashMap();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog().withCatalogConf(conf).withCatalogEntity(entity);

    ViewCatalog viewCatalog = icebergCatalog.asViewCatalog();
    Assertions.assertNotNull(viewCatalog);
    Assertions.assertTrue(viewCatalog instanceof IcebergCatalogOperations);
  }

  @Test
  void testJdbcBackendDefaultCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC backend with jdbc-user and jdbc-password
    Map<String, String> jdbcProps = Maps.newHashMap();
    jdbcProps.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    jdbcProps.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    jdbcProps.put(IcebergConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcProps.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "test-password");

    CatalogEntity jdbcEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("jdbc-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcProps)
            .build();

    IcebergCatalog jdbcCatalog =
        new IcebergCatalog().withCatalogConf(jdbcProps).withCatalogEntity(jdbcEntity);
    Map<String, String> properties = jdbcCatalog.propertiesWithCredentialProviders();

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
    jdbcS3Props.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    jdbcS3Props.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    jdbcS3Props.put(IcebergConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcS3Props.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    jdbcS3Props.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "access-key");
    jdbcS3Props.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "secret-key");

    CatalogEntity jdbcS3Entity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("jdbc-s3-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcS3Props)
            .build();

    IcebergCatalog jdbcS3Catalog =
        new IcebergCatalog().withCatalogConf(jdbcS3Props).withCatalogEntity(jdbcS3Entity);
    Map<String, String> properties = jdbcS3Catalog.propertiesWithCredentialProviders();

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

    // Test non-JDBC backend (hive) - should not add default credential providers
    Map<String, String> hiveProps = Maps.newHashMap();
    hiveProps.put(IcebergConstants.CATALOG_BACKEND, "hive");
    hiveProps.put(IcebergConstants.URI, "thrift://localhost:9083");

    CatalogEntity hiveEntity =
        CatalogEntity.builder()
            .withId(3L)
            .withName("hive-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(hiveProps)
            .build();

    IcebergCatalog hiveCatalog =
        new IcebergCatalog().withCatalogConf(hiveProps).withCatalogEntity(hiveEntity);
    Map<String, String> properties = hiveCatalog.propertiesWithCredentialProviders();

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
    explicitProps.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    explicitProps.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    explicitProps.put(IcebergConstants.GRAVITINO_JDBC_USER, "test-user");
    explicitProps.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    explicitProps.put(CredentialConstants.CREDENTIAL_PROVIDERS, "custom-provider");

    CatalogEntity explicitEntity =
        CatalogEntity.builder()
            .withId(4L)
            .withName("explicit-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(explicitProps)
            .build();

    IcebergCatalog explicitCatalog =
        new IcebergCatalog().withCatalogConf(explicitProps).withCatalogEntity(explicitEntity);
    Map<String, String> properties = explicitCatalog.propertiesWithCredentialProviders();

    // Should keep explicit credential providers, not override
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertEquals("custom-provider", credentialProviders);
  }

  @Test
  void testJdbcBackendWithOSSCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC backend with jdbc-user, jdbc-password, and OSS credentials
    Map<String, String> jdbcOssProps = Maps.newHashMap();
    jdbcOssProps.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    jdbcOssProps.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    jdbcOssProps.put(IcebergConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcOssProps.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    jdbcOssProps.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, "oss-access-key");
    jdbcOssProps.put(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, "oss-secret-key");

    CatalogEntity jdbcOssEntity =
        CatalogEntity.builder()
            .withId(5L)
            .withName("jdbc-oss-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcOssProps)
            .build();

    IcebergCatalog jdbcOssCatalog =
        new IcebergCatalog().withCatalogConf(jdbcOssProps).withCatalogEntity(jdbcOssEntity);
    Map<String, String> properties = jdbcOssCatalog.propertiesWithCredentialProviders();

    // Should have both jdbc and oss-secret-key credential providers
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNotNull(credentialProviders);
    Assertions.assertTrue(credentialProviders.contains(JdbcCredential.JDBC_CREDENTIAL_TYPE));
    Assertions.assertTrue(
        credentialProviders.contains(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE));
  }

  @Test
  void testJdbcBackendWithAzureCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC backend with jdbc-user, jdbc-password, and Azure credentials
    Map<String, String> jdbcAzureProps = Maps.newHashMap();
    jdbcAzureProps.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    jdbcAzureProps.put(IcebergConstants.URI, "jdbc:sqlite::memory:");
    jdbcAzureProps.put(IcebergConstants.GRAVITINO_JDBC_USER, "test-user");
    jdbcAzureProps.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "test-password");
    jdbcAzureProps.put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME, "azure-account-name");
    jdbcAzureProps.put(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY, "azure-account-key");

    CatalogEntity jdbcAzureEntity =
        CatalogEntity.builder()
            .withId(6L)
            .withName("jdbc-azure-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(IcebergCatalog.Type.RELATIONAL)
            .withProvider("iceberg")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcAzureProps)
            .build();

    IcebergCatalog jdbcAzureCatalog =
        new IcebergCatalog().withCatalogConf(jdbcAzureProps).withCatalogEntity(jdbcAzureEntity);
    Map<String, String> properties = jdbcAzureCatalog.propertiesWithCredentialProviders();

    // Should have both jdbc and azure-account-key credential providers
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNotNull(credentialProviders);
    Assertions.assertTrue(credentialProviders.contains(JdbcCredential.JDBC_CREDENTIAL_TYPE));
    Assertions.assertTrue(
        credentialProviders.contains(AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE));
  }
}
