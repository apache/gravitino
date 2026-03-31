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

package org.apache.gravitino.iceberg.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogWrapperForREST {

  @Test
  void testCheckPropertiesForCompatibility() {
    ImmutableMap<String, String> deprecatedMap = ImmutableMap.of("deprecated", "new");
    ImmutableMap<String, String> propertiesWithDeprecatedKey = ImmutableMap.of("deprecated", "v");
    Map<String, String> newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("new", "v"));

    ImmutableMap<String, String> propertiesWithoutDeprecatedKey = ImmutableMap.of("k", "v");
    newProperties =
        CatalogWrapperForREST.checkForCompatibility(propertiesWithoutDeprecatedKey, deprecatedMap);
    Assertions.assertEquals(newProperties, ImmutableMap.of("k", "v"));

    ImmutableMap<String, String> propertiesWithBothKey =
        ImmutableMap.of("deprecated", "v", "new", "v");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.checkForCompatibility(propertiesWithBothKey, deprecatedMap));
  }

  @Test
  void testIsLocalOrHdfsLocation() {
    Assertions.assertTrue(CatalogWrapperForREST.isLocalOrHdfsLocation("/tmp/warehouse"));
    Assertions.assertTrue(CatalogWrapperForREST.isLocalOrHdfsLocation("file:///tmp/warehouse"));
    Assertions.assertTrue(
        CatalogWrapperForREST.isLocalOrHdfsLocation("hdfs://localhost:9000/warehouse"));

    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation("s3://bucket/warehouse"));
    Assertions.assertFalse(
        CatalogWrapperForREST.isLocalOrHdfsLocation("abfs://container@account/warehouse"));
    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation(""));
    Assertions.assertFalse(CatalogWrapperForREST.isLocalOrHdfsLocation("   "));
  }

  @Test
  void testValidateCredentialLocation() {
    Assertions.assertDoesNotThrow(
        () -> CatalogWrapperForREST.validateCredentialLocation("/tmp/warehouse"));
    Assertions.assertDoesNotThrow(
        () -> CatalogWrapperForREST.validateCredentialLocation("file:///tmp/warehouse"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> CatalogWrapperForREST.validateCredentialLocation(""));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.validateCredentialLocation("   "));
  }

  @Test
  void testCatalogConfigToClientsForRestBackendUsesMergedRemoteProperties() {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "rest",
                IcebergConstants.URI,
                "http://client-config-only:8181"));

    RESTCatalog restCatalog = mock(RESTCatalog.class);
    when(restCatalog.properties())
        .thenReturn(
            ImmutableMap.of(
                IcebergConstants.URI,
                "http://merged-from-remote-config:9999",
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO",
                IcebergConstants.ICEBERG_S3_ENDPOINT,
                "http://localhost:9000",
                IcebergConstants.DATA_ACCESS,
                "vended-credentials",
                IcebergConstants.WAREHOUSE,
                "/remote/warehouse"));

    Map<String, String> configToClients =
        CatalogWrapperForREST.buildCatalogConfigToClients(config, restCatalog);

    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", configToClients.get(IcebergConstants.IO_IMPL));
    Assertions.assertEquals(
        "http://localhost:9000", configToClients.get(IcebergConstants.ICEBERG_S3_ENDPOINT));
    Assertions.assertEquals(
        "vended-credentials", configToClients.get(IcebergConstants.DATA_ACCESS));
  }

  @Test
  void testCatalogConfigToClientsForNonRestBackend() {
    Catalog catalog = mock(Catalog.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "hive",
                IcebergConstants.URI,
                "thrift://hive-metastore:9083",
                IcebergConstants.IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO"));
    Map<String, String> configToClients =
        CatalogWrapperForREST.buildCatalogConfigToClients(config, catalog);
    Assertions.assertFalse(configToClients.containsKey(IcebergConstants.URI));
    Assertions.assertEquals(
        "org.apache.iceberg.aws.s3.S3FileIO", configToClients.get(IcebergConstants.IO_IMPL));
    Assertions.assertFalse(configToClients.containsKey(IcebergConstants.DATA_ACCESS));
  }

  @Test
  void testCatalogConfigToClientsRejectsInvalidDataAccessValue() {
    Catalog catalog = mock(Catalog.class);
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.CATALOG_BACKEND,
                "hive",
                IcebergConstants.DATA_ACCESS,
                "invalid-mode"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> CatalogWrapperForREST.buildCatalogConfigToClients(config, catalog));
  }
}
