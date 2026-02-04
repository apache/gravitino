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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;

public class TestPaimonCatalogPropertiesMetadata {

  @Test
  public void testJdbcUsernamePropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> usernameEntry = properties.get(PaimonConstants.GRAVITINO_JDBC_USER);
    assertNotNull(usernameEntry, "JDBC username property should be defined");
    assertTrue(usernameEntry.isSensitive(), "JDBC username should be marked as sensitive");
    assertFalse(usernameEntry.isRequired(), "JDBC username should be optional");
    assertFalse(usernameEntry.isImmutable(), "JDBC username should not be immutable");
    assertFalse(usernameEntry.isHidden(), "JDBC username should not be hidden");
  }

  @Test
  public void testJdbcPasswordPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> passwordEntry = properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD);
    assertNotNull(passwordEntry, "JDBC password property should be defined");
    assertTrue(passwordEntry.isSensitive(), "JDBC password should be marked as sensitive");
    assertFalse(passwordEntry.isRequired(), "JDBC password should be optional");
    assertFalse(passwordEntry.isImmutable(), "JDBC password should not be immutable");
    assertFalse(passwordEntry.isHidden(), "JDBC password should not be hidden");
  }

  @Test
  public void testRestTokenPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> tokenEntry = properties.get(PaimonConstants.TOKEN);
    assertNotNull(tokenEntry, "REST token property should be defined");
    assertTrue(tokenEntry.isSensitive(), "REST token should be marked as sensitive");
    assertFalse(tokenEntry.isRequired(), "REST token should be optional");
    assertFalse(tokenEntry.isImmutable(), "REST token should not be immutable");
    assertFalse(tokenEntry.isHidden(), "REST token should not be hidden");
  }

  @Test
  public void testDlfAccessKeySecretPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> secretEntry = properties.get(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET);
    assertNotNull(secretEntry, "DLF access key secret property should be defined");
    assertTrue(secretEntry.isSensitive(), "DLF access key secret should be marked as sensitive");
    assertFalse(secretEntry.isRequired(), "DLF access key secret should be optional");
    assertFalse(secretEntry.isImmutable(), "DLF access key secret should not be immutable");
    assertFalse(secretEntry.isHidden(), "DLF access key secret should not be hidden");
  }

  @Test
  public void testDlfSecurityTokenPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> tokenEntry = properties.get(PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN);
    assertNotNull(tokenEntry, "DLF security token property should be defined");
    assertTrue(tokenEntry.isSensitive(), "DLF security token should be marked as sensitive");
    assertFalse(tokenEntry.isRequired(), "DLF security token should be optional");
    assertFalse(tokenEntry.isImmutable(), "DLF security token should not be immutable");
    assertFalse(tokenEntry.isHidden(), "DLF security token should not be hidden");
  }

  @Test
  public void testDlfAccessKeyIdPropertyIsNotSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> accessKeyIdEntry = properties.get(PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID);
    assertNotNull(accessKeyIdEntry, "DLF access key ID property should be defined");
    assertFalse(
        accessKeyIdEntry.isSensitive(), "DLF access key ID should not be marked as sensitive");
  }

  @Test
  public void testS3AccessKeyPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> s3AccessKeyEntry = properties.get(PaimonConstants.S3_ACCESS_KEY);
    assertNotNull(s3AccessKeyEntry, "S3 access key property should be defined");
    assertTrue(s3AccessKeyEntry.isSensitive(), "S3 access key should be marked as sensitive");
    assertFalse(s3AccessKeyEntry.isRequired(), "S3 access key should be optional");
  }

  @Test
  public void testS3SecretKeyPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> s3SecretKeyEntry = properties.get(PaimonConstants.S3_SECRET_KEY);
    assertNotNull(s3SecretKeyEntry, "S3 secret key property should be defined");
    assertTrue(s3SecretKeyEntry.isSensitive(), "S3 secret key should be marked as sensitive");
    assertFalse(s3SecretKeyEntry.isRequired(), "S3 secret key should be optional");
  }

  @Test
  public void testOssAccessKeyPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> ossAccessKeyEntry = properties.get(PaimonConstants.OSS_ACCESS_KEY);
    assertNotNull(ossAccessKeyEntry, "OSS access key property should be defined");
    assertTrue(ossAccessKeyEntry.isSensitive(), "OSS access key should be marked as sensitive");
    assertFalse(ossAccessKeyEntry.isRequired(), "OSS access key should be optional");
  }

  @Test
  public void testOssSecretKeyPropertyIsSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> ossSecretKeyEntry = properties.get(PaimonConstants.OSS_SECRET_KEY);
    assertNotNull(ossSecretKeyEntry, "OSS secret key property should be defined");
    assertTrue(ossSecretKeyEntry.isSensitive(), "OSS secret key should be marked as sensitive");
    assertFalse(ossSecretKeyEntry.isRequired(), "OSS secret key should be optional");
  }

  @Test
  public void testWarehousePropertyIsNotSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> warehouseEntry = properties.get(PaimonConstants.WAREHOUSE);
    assertNotNull(warehouseEntry, "Warehouse property should be defined");
    assertFalse(warehouseEntry.isSensitive(), "Warehouse should not be marked as sensitive");
    assertTrue(warehouseEntry.isRequired(), "Warehouse should be required");
  }

  @Test
  public void testUriPropertyIsNotSensitive() {
    PaimonCatalogPropertiesMetadata metadata = new PaimonCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> uriEntry = properties.get(PaimonConstants.URI);
    assertNotNull(uriEntry, "URI property should be defined");
    assertFalse(uriEntry.isSensitive(), "URI should not be marked as sensitive");
    assertFalse(uriEntry.isRequired(), "URI should be optional");
  }
}
