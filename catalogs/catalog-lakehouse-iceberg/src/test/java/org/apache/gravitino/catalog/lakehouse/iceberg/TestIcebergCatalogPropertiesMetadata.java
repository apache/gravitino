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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestIcebergCatalogPropertiesMetadata {

  @Test
  public void testJdbcUsernamePropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> usernameEntry = properties.get(IcebergConstants.GRAVITINO_JDBC_USER);
    assertNotNull(usernameEntry, "JDBC username property should be defined");
    assertTrue(usernameEntry.isSensitive(), "JDBC username should be marked as sensitive");
    assertFalse(usernameEntry.isRequired(), "JDBC username should be optional");
    assertFalse(usernameEntry.isImmutable(), "JDBC username should not be immutable");
    assertFalse(usernameEntry.isHidden(), "JDBC username should not be hidden");
  }

  @Test
  public void testJdbcPasswordPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> passwordEntry = properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD);
    assertNotNull(passwordEntry, "JDBC password property should be defined");
    assertTrue(passwordEntry.isSensitive(), "JDBC password should be marked as sensitive");
    assertFalse(passwordEntry.isRequired(), "JDBC password should be optional");
    assertFalse(passwordEntry.isImmutable(), "JDBC password should not be immutable");
    assertFalse(passwordEntry.isHidden(), "JDBC password should not be hidden");
  }

  @Test
  public void testS3AccessKeyIdPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> accessKeyEntry = properties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
    assertNotNull(accessKeyEntry, "S3 access key ID property should be defined");
    assertTrue(accessKeyEntry.isSensitive(), "S3 access key ID should be marked as sensitive");
    assertFalse(accessKeyEntry.isRequired(), "S3 access key ID should be optional");
    assertFalse(accessKeyEntry.isImmutable(), "S3 access key ID should not be immutable");
  }

  @Test
  public void testS3SecretAccessKeyPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> secretKeyEntry = properties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    assertNotNull(secretKeyEntry, "S3 secret access key property should be defined");
    assertTrue(secretKeyEntry.isSensitive(), "S3 secret access key should be marked as sensitive");
    assertFalse(secretKeyEntry.isRequired(), "S3 secret access key should be optional");
    assertFalse(secretKeyEntry.isImmutable(), "S3 secret access key should not be immutable");
  }

  @Test
  public void testOssAccessKeyIdPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> accessKeyEntry = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
    assertNotNull(accessKeyEntry, "OSS access key ID property should be defined");
    assertTrue(accessKeyEntry.isSensitive(), "OSS access key ID should be marked as sensitive");
    assertFalse(accessKeyEntry.isRequired(), "OSS access key ID should be optional");
    assertFalse(accessKeyEntry.isImmutable(), "OSS access key ID should not be immutable");
  }

  @Test
  public void testOssAccessKeySecretPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> secretKeyEntry = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
    assertNotNull(secretKeyEntry, "OSS access key secret property should be defined");
    assertTrue(secretKeyEntry.isSensitive(), "OSS access key secret should be marked as sensitive");
    assertFalse(secretKeyEntry.isRequired(), "OSS access key secret should be optional");
    assertFalse(secretKeyEntry.isImmutable(), "OSS access key secret should not be immutable");
  }

  @Test
  public void testAzureStorageAccountNamePropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> accountNameEntry =
        properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    assertNotNull(accountNameEntry, "Azure storage account name property should be defined");
    assertTrue(
        accountNameEntry.isSensitive(), "Azure storage account name should be marked as sensitive");
    assertFalse(accountNameEntry.isRequired(), "Azure storage account name should be optional");
    assertFalse(
        accountNameEntry.isImmutable(), "Azure storage account name should not be immutable");
  }

  @Test
  public void testAzureStorageAccountKeyPropertyIsSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> accountKeyEntry =
        properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    assertNotNull(accountKeyEntry, "Azure storage account key property should be defined");
    assertTrue(
        accountKeyEntry.isSensitive(), "Azure storage account key should be marked as sensitive");
    assertFalse(accountKeyEntry.isRequired(), "Azure storage account key should be optional");
    assertFalse(accountKeyEntry.isImmutable(), "Azure storage account key should not be immutable");
  }

  @Test
  public void testCatalogBackendPropertyIsNotSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> backendEntry = properties.get(IcebergConstants.CATALOG_BACKEND);
    assertNotNull(backendEntry, "Catalog backend property should be defined");
    assertFalse(backendEntry.isSensitive(), "Catalog backend should not be marked as sensitive");
    assertTrue(backendEntry.isRequired(), "Catalog backend should be required");
    assertTrue(backendEntry.isImmutable(), "Catalog backend should be immutable");
  }

  @Test
  public void testUriPropertyIsNotSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> uriEntry = properties.get(IcebergConstants.URI);
    assertNotNull(uriEntry, "URI property should be defined");
    assertFalse(uriEntry.isSensitive(), "URI should not be marked as sensitive");
    assertTrue(uriEntry.isRequired(), "URI should be required");
  }

  @Test
  public void testWarehousePropertyIsNotSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> warehouseEntry = properties.get(IcebergConstants.WAREHOUSE);
    assertNotNull(warehouseEntry, "Warehouse property should be defined");
    assertFalse(warehouseEntry.isSensitive(), "Warehouse should not be marked as sensitive");
    assertFalse(warehouseEntry.isRequired(), "Warehouse should be optional");
  }

  @Test
  public void testTransformPropertiesWithCredentials() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, String> inputProperties = Maps.newHashMap();
    inputProperties.put(IcebergConstants.CATALOG_BACKEND, "jdbc");
    inputProperties.put(IcebergConstants.URI, "jdbc:postgresql://localhost:5432/db");
    inputProperties.put(IcebergConstants.GRAVITINO_JDBC_USER, "testuser");
    inputProperties.put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "testpass");
    inputProperties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, "s3-access-key");
    inputProperties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, "s3-secret-key");

    Map<String, String> result = metadata.transformProperties(inputProperties);

    // Verify that credentials are transformed to Iceberg format
    assertTrue(
        result.containsKey(IcebergConstants.ICEBERG_JDBC_USER),
        "Username should be transformed to Iceberg format");
    assertTrue(
        result.containsKey(IcebergConstants.ICEBERG_JDBC_PASSWORD),
        "Password should be transformed to Iceberg format");
    assertEquals("testuser", result.get(IcebergConstants.ICEBERG_JDBC_USER));
    assertEquals("testpass", result.get(IcebergConstants.ICEBERG_JDBC_PASSWORD));
  }

  @Test
  public void testAllSensitivePropertiesHaveDescriptions() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    // JDBC credentials
    PropertyEntry<?> username = properties.get(IcebergConstants.GRAVITINO_JDBC_USER);
    PropertyEntry<?> password = properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD);

    assertNotNull(username.getDescription(), "Username should have a description");
    assertNotNull(password.getDescription(), "Password should have a description");
    assertEquals("JDBC username", username.getDescription());
    assertEquals("JDBC password", password.getDescription());

    // S3 credentials
    PropertyEntry<?> s3AccessKey = properties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
    PropertyEntry<?> s3SecretKey = properties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);

    assertNotNull(s3AccessKey.getDescription(), "S3 access key should have a description");
    assertNotNull(s3SecretKey.getDescription(), "S3 secret key should have a description");
  }

  @Test
  public void testTableMetadataCachePropertiesAreNotSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> cacheImpl = properties.get(IcebergConstants.TABLE_METADATA_CACHE_IMPL);
    PropertyEntry<?> cacheCapacity = properties.get(IcebergConstants.TABLE_METADATA_CACHE_CAPACITY);
    PropertyEntry<?> cacheExpire =
        properties.get(IcebergConstants.TABLE_METADATA_CACHE_EXPIRE_MINUTES);

    assertNotNull(cacheImpl, "Cache implementation property should be defined");
    assertNotNull(cacheCapacity, "Cache capacity property should be defined");
    assertNotNull(cacheExpire, "Cache expire property should be defined");

    assertFalse(cacheImpl.isSensitive(), "Cache implementation should not be sensitive");
    assertFalse(cacheCapacity.isSensitive(), "Cache capacity should not be sensitive");
    assertFalse(cacheExpire.isSensitive(), "Cache expire should not be sensitive");
  }

  @Test
  public void testIoImplPropertyIsNotSensitive() {
    IcebergCatalogPropertiesMetadata metadata = new IcebergCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> ioImplEntry = properties.get(IcebergConstants.IO_IMPL);
    assertNotNull(ioImplEntry, "IO implementation property should be defined");
    assertFalse(ioImplEntry.isSensitive(), "IO implementation should not be marked as sensitive");
    assertFalse(ioImplEntry.isRequired(), "IO implementation should be optional");
    assertTrue(ioImplEntry.isImmutable(), "IO implementation should be immutable");
  }
}
