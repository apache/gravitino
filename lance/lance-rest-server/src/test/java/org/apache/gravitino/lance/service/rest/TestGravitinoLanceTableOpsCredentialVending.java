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
package org.apache.gravitino.lance.service.rest;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceNamespaceWrapper;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.model.DescribeTableResponse;
import org.mockito.ArgumentCaptor;

class TestGravitinoLanceTableOpsCredentialVending {

  private static final String CATALOG_NAME = "testCatalog";
  private static final String SCHEMA_NAME = "testSchema";
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_ID = CATALOG_NAME + "$" + SCHEMA_NAME + "$" + TABLE_NAME;
  private static final String DELIMITER = "$";
  private static final String TABLE_LOCATION = "s3://test-bucket/path/to/table";

  private GravitinoLanceNamespaceWrapper namespaceWrapper;
  private Catalog catalog;
  private TableCatalog tableCatalog;
  private Table table;
  private CatalogCredentialManager credentialManager;
  private GravitinoLanceTableOperations ops;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    namespaceWrapper = mock(GravitinoLanceNamespaceWrapper.class);
    catalog = mock(Catalog.class);
    tableCatalog = mock(TableCatalog.class);
    table = mock(Table.class);
    credentialManager = mock(CatalogCredentialManager.class);

    ops = new GravitinoLanceTableOperations(namespaceWrapper);

    // Inject mock CatalogCredentialManager via reflection so computeIfAbsent uses the mock
    Field field = GravitinoLanceTableOperations.class.getDeclaredField("credentialManagers");
    field.setAccessible(true);
    ConcurrentHashMap<String, CatalogCredentialManager> map =
        (ConcurrentHashMap<String, CatalogCredentialManager>) field.get(ops);
    map.put(CATALOG_NAME, credentialManager);

    // Common mock setup
    when(namespaceWrapper.loadAndValidateLakehouseCatalog(CATALOG_NAME)).thenReturn(catalog);
    when(catalog.asTableCatalog()).thenReturn(tableCatalog);
    when(tableCatalog.loadTable(NameIdentifier.of(SCHEMA_NAME, TABLE_NAME))).thenReturn(table);
    when(table.columns()).thenReturn(new Column[] {});
    when(table.properties())
        .thenReturn(
            Map.of(
                LANCE_LOCATION,
                TABLE_LOCATION,
                LANCE_TABLE_VERSION,
                "5",
                "lance.storage.region",
                "us-east-1"));
  }

  @Test
  void testDescribeTableWithCredentialVending() {
    S3TokenCredential s3Credential = new S3TokenCredential("ak", "sk", "session-token", 12345L);
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class)))
        .thenReturn(s3Credential);

    DescribeTableResponse response =
        ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.WRITE);

    // Verify vended credential keys
    Map<String, String> storageOptions = response.getStorageOptions();
    assertNotNull(storageOptions);
    assertEquals("ak", storageOptions.get("aws_access_key_id"));
    assertEquals("sk", storageOptions.get("aws_secret_access_key"));
    assertEquals("session-token", storageOptions.get("aws_session_token"));
    assertEquals("12345", storageOptions.get("expires_at_millis"));

    // Verify base storage options are preserved after merge
    assertEquals("us-east-1", storageOptions.get("region"));

    // Verify other response fields
    assertEquals(TABLE_LOCATION, response.getLocation());
    assertEquals(5L, response.getVersion());
  }

  @Test
  void testDescribeTableWithoutCredentialVending() {
    DescribeTableResponse response = ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), null);

    Map<String, String> storageOptions = response.getStorageOptions();
    assertNotNull(storageOptions);
    // Should only contain base storage options (prefixed keys stripped)
    assertEquals("us-east-1", storageOptions.get("region"));
    // No credential keys
    assertNull(storageOptions.get("aws_access_key_id"));
    assertNull(storageOptions.get("aws_secret_access_key"));
    assertNull(storageOptions.get("aws_session_token"));
  }

  @Test
  void testDescribeTableCredentialFallbackOnNoProviders() {
    // Simulates catalog with no credential providers configured
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class)))
        .thenThrow(new IllegalArgumentException("No credential providers configured"));

    DescribeTableResponse response =
        ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.READ);

    // Should fall back to original storage options
    Map<String, String> storageOptions = response.getStorageOptions();
    assertNotNull(storageOptions);
    assertEquals("us-east-1", storageOptions.get("region"));
    assertNull(storageOptions.get("aws_access_key_id"));
  }

  @Test
  void testDescribeTableNullCredentialThrowsServiceUnavailable() {
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class))).thenReturn(null);

    assertThrows(
        ServiceUnavailableException.class,
        () -> ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.WRITE));
  }

  @Test
  void testDescribeTableWritePrivilegePassesWritePaths() {
    S3TokenCredential s3Credential = new S3TokenCredential("ak", "sk", "token", 100L);
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class)))
        .thenReturn(s3Credential);

    ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.WRITE);

    ArgumentCaptor<PathBasedCredentialContext> captor =
        ArgumentCaptor.forClass(PathBasedCredentialContext.class);
    verify(credentialManager).getCredential(captor.capture());

    PathBasedCredentialContext context = captor.getValue();
    assertTrue(context.getWritePaths().contains(TABLE_LOCATION));
    assertTrue(context.getReadPaths().isEmpty());
  }

  @Test
  void testDescribeTableReadPrivilegePassesReadPaths() {
    S3TokenCredential s3Credential = new S3TokenCredential("ak", "sk", "token", 100L);
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class)))
        .thenReturn(s3Credential);

    ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.READ);

    ArgumentCaptor<PathBasedCredentialContext> captor =
        ArgumentCaptor.forClass(PathBasedCredentialContext.class);
    verify(credentialManager).getCredential(captor.capture());

    PathBasedCredentialContext context = captor.getValue();
    assertTrue(context.getReadPaths().contains(TABLE_LOCATION));
    assertTrue(context.getWritePaths().isEmpty());
  }

  @Test
  void testDescribeTableCredentialVendingMergesWithExistingOptions() {
    // Table has both base storage options and will receive vended credentials
    when(table.properties())
        .thenReturn(
            Map.of(
                LANCE_LOCATION,
                TABLE_LOCATION,
                "lance.storage.region",
                "us-west-2",
                "lance.storage.endpoint",
                "https://s3.example.com"));

    S3TokenCredential s3Credential = new S3TokenCredential("ak", "sk", "token", 500L);
    when(credentialManager.getCredential(any(PathBasedCredentialContext.class)))
        .thenReturn(s3Credential);

    DescribeTableResponse response =
        ops.describeTable(TABLE_ID, DELIMITER, Optional.empty(), CredentialPrivilege.WRITE);

    Map<String, String> storageOptions = response.getStorageOptions();
    // Base options preserved
    assertEquals("us-west-2", storageOptions.get("region"));
    assertEquals("https://s3.example.com", storageOptions.get("endpoint"));
    // Vended credentials added
    assertEquals("ak", storageOptions.get("aws_access_key_id"));
    assertEquals("sk", storageOptions.get("aws_secret_access_key"));
    assertEquals("token", storageOptions.get("aws_session_token"));
    assertEquals("500", storageOptions.get("expires_at_millis"));
  }
}
