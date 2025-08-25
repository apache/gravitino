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
package org.apache.gravitino.flink.connector.store;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.junit.Before;
import org.junit.Test;

public class TestGravitinoCatalogStore {
  private GravitinoCatalogManager gravitinoCatalogMockManager;
  private GravitinoCatalogStore gravitinoCatalogStore;

  @Before
  public void setupCatalogStore() {
    gravitinoCatalogMockManager = mock(GravitinoCatalogManager.class);
    gravitinoCatalogStore = new GravitinoCatalogStore(gravitinoCatalogMockManager);
  }

  @Test
  public void testRemoveCatalog_whenCatalogExists_shouldSucceed() {
    String catalogName = "testCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(true);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_whenCatalogNotExists_ignoreFlagTrue_shouldNotThrow() {
    String catalogName = "missingCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(false);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, true);
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_whenCatalogNotExists_ignoreFlagFalse_shouldThrow() {
    String catalogName = "missingCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName)).thenReturn(false);
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
      fail("Expected CatalogException to be thrown");
    } catch (CatalogException e) {
      assertTrue(
          "Expected failure message to contain 'Failed to remove the catalog:'",
          e.getMessage().contains("Failed to remove the catalog:"));
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }

  @Test
  public void testRemoveCatalog_UnexpectedException_shouldThrow() {
    String catalogName = "errorCatalog";
    when(gravitinoCatalogMockManager.dropCatalog(catalogName))
        .thenThrow(new RuntimeException("UnexpectedErrorOccurred"));
    try {
      gravitinoCatalogStore.removeCatalog(catalogName, false);
      fail("Expected CatalogException to be thrown");
    } catch (CatalogException e) {
      assertTrue(
          "Expected failure message to contain 'Failed to remove the catalog:'",
          e.getMessage().contains("Failed to remove the catalog:"));
      assertTrue("Expected cause to be RuntimeException", e.getCause() instanceof RuntimeException);
    }
    verify(gravitinoCatalogMockManager).dropCatalog(catalogName);
  }
}
