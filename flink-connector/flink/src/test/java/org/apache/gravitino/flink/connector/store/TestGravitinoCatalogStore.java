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
