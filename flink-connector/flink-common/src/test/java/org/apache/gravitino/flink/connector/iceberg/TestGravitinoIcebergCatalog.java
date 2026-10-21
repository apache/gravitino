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

package org.apache.gravitino.flink.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.storage.GCSProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestGravitinoIcebergCatalog {

  @Test
  void testRequireAbstractCatalogAcceptsAbstractCatalog() {
    AbstractCatalog catalog = mock(AbstractCatalog.class);

    assertSame(catalog, GravitinoIcebergCatalog.asAbstractCatalog(catalog));
  }

  @Test
  void testRequireAbstractCatalogRejectsNonAbstractCatalog() {
    Catalog catalog = mock(Catalog.class);

    assertThrows(
        IllegalStateException.class, () -> GravitinoIcebergCatalog.asAbstractCatalog(catalog));
  }

  @Test
  void testCheckCredentialProvidersRejectsAwsIrsaForNonRestBackend() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "hive",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "s3-secret-key, AWS-IRSA");

    CatalogException exception =
        assertThrows(
            CatalogException.class,
            () -> GravitinoIcebergCatalog.checkCredentialProviders("c1", "hive", properties));
    assertTrue(exception.getMessage().contains("aws-irsa"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3-token", "gcs-token", "adls-token", "oss-token", "cos-token"})
  void testCheckCredentialProvidersReturnsTokenProvidersForNonRestBackend(String provider) {
    String upperCaseProvider = provider.toUpperCase(Locale.ROOT);
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "hive",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "s3-secret-key, " + upperCaseProvider);

    assertEquals(
        ImmutableList.of(upperCaseProvider),
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "hive", properties));
  }

  @Test
  void testCheckCredentialProvidersIgnoresStaticProviders() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "jdbc",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "s3-secret-key,jdbc-user-password");

    assertTrue(GravitinoIcebergCatalog.checkCredentialProviders("c1", null, properties).isEmpty());
  }

  @Test
  void testCheckCredentialProvidersIgnoresMissingProviders() {
    Map<String, String> properties =
        ImmutableMap.of(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND, "hive");

    assertTrue(
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "hive", properties).isEmpty());
  }

  @Test
  void testCheckCredentialProvidersInfersGcsTokenFromServiceAccountFile() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "hive",
            GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE,
            "/path/to/sa.json");

    assertEquals(
        ImmutableList.of(GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE),
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "hive", properties));
  }

  @Test
  void testCheckCredentialProvidersUsesExplicitProvidersOverGcsInference() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "hive",
            GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE,
            "/path/to/sa.json",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "s3-secret-key");

    assertTrue(
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "hive", properties).isEmpty());
  }

  @Test
  void testCheckCredentialProvidersSkipsRestBackend() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "REST",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "s3-token,aws-irsa");

    assertTrue(
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "REST", properties).isEmpty());
  }

  @Test
  void testCheckCredentialProvidersUsesEffectiveCatalogType() {
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            "hive",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "aws-irsa");

    assertTrue(
        GravitinoIcebergCatalog.checkCredentialProviders("c1", "rest", properties).isEmpty());
  }
}
