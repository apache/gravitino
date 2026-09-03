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
package org.apache.gravitino.trino.connector.system.table;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogRegistrationState;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

public class TestGravitinoSystemStatusTables {

  @Test
  public void testCatalogTableReportsOnlyItsOwnMetalakeInMultiMetalakeMode() {
    // gravitino.system.catalog must report only its entry catalog's metalake, not every metalake
    // the manager happens to have loaded.
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.getUsedMetalakes()).thenReturn(Set.of("prod", "dev"));

    GravitinoMetalake prodMetalake = mock(GravitinoMetalake.class);
    Catalog prodCatalog = mock(Catalog.class);
    when(prodCatalog.name()).thenReturn("memory");
    when(prodCatalog.provider()).thenReturn("memory");
    when(prodCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(prodCatalog.properties()).thenReturn(Map.of());
    Audit audit = mock(Audit.class);
    when(audit.createTime()).thenReturn(Instant.now());
    when(prodCatalog.auditInfo()).thenReturn(audit);
    when(prodMetalake.listCatalogsInfo()).thenReturn(new Catalog[] {prodCatalog});
    when(manager.getMetalake("prod")).thenReturn(prodMetalake);
    when(manager.getTrinoCatalogName("prod", "memory")).thenReturn("memory");
    when(manager.skipCatalog("memory")).thenReturn(false);

    // "dev" is a used metalake too, but this table instance is scoped to "prod" and must not
    // even look at "dev": leaving its mock unstubbed means any access throws or returns null,
    // which would surface as a test failure instead of silently leaking dev's catalogs in.

    Page page = new GravitinoSystemTableCatalog(manager, "prod").loadPageData();

    assertEquals(1, page.getPositionCount());
    assertEquals("memory", varchar(page, 0));
  }

  @Test
  public void testCatalogStatusTableRendersRegisteredCatalog() {
    GravitinoCatalog catalog =
        new GravitinoCatalog("test", "memory", "memory", ImmutableMap.of(), 0L);
    CatalogRegistrationState state = CatalogRegistrationState.succeeded(catalog, "memory");

    Page page = loadCatalogStatusPage(List.of(state));

    assertEquals(1, page.getPositionCount());
    assertEquals(9, page.getChannelCount());
    assertEquals("test", varchar(page, 0));
    assertEquals("memory", varchar(page, 1));
    assertEquals("memory", varchar(page, 2));
    assertEquals("memory", varchar(page, 3));
    assertEquals("REGISTERED", varchar(page, 4));
    // last_error is null for a registered catalog, last_success_time is set.
    assertTrue(page.getBlock(5).isNull(0));
    assertFalse(page.getBlock(7).isNull(0));
    assertEquals(0, BIGINT.getLong(page.getBlock(8), 0));
  }

  @Test
  public void testCatalogStatusTableRendersFailedCatalog() {
    CatalogRegistrationState state =
        CatalogRegistrationState.failed(
            "test", "memory", "memory", null, "Access Denied: Cannot create catalog memory");

    Page page = loadCatalogStatusPage(List.of(state));

    assertEquals("FAILED", varchar(page, 4));
    assertTrue(varchar(page, 5).contains("Access Denied"));
    // The provider is unknown on this path, and the catalog was never registered.
    assertTrue(page.getBlock(3).isNull(0));
    assertTrue(page.getBlock(7).isNull(0));
    assertEquals(1, BIGINT.getLong(page.getBlock(8), 0));
  }

  @Test
  public void testCatalogStatusTableIsEmptyWhenNoCatalogWasSeen() {
    Page page = loadCatalogStatusPage(List.of());
    assertEquals(0, page.getPositionCount());
  }

  @Test
  public void testLoadStatusTableRendersHealthyLoop() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.isTrinoStarted()).thenReturn(true);
    when(manager.getLastLoadAttemptTimeMs()).thenReturn(1000L);
    when(manager.getLoadOutcome())
        .thenReturn(new CatalogConnectorManager.LoadOutcome(1000L, null, 0L));
    when(manager.getMetalakeErrors()).thenReturn(Map.of());

    Page page = new GravitinoSystemTableLoadStatus(manager, "test").loadPageData();

    assertEquals(1, page.getPositionCount());
    assertEquals(6, page.getChannelCount());
    assertTrue(BOOLEAN.getBoolean(page.getBlock(0), 0));
    assertEquals("1970-01-01T00:00:01Z", varchar(page, 1));
    assertEquals(0, BIGINT.getLong(page.getBlock(3), 0));
    assertTrue(page.getBlock(4).isNull(0));
    assertTrue(page.getBlock(5).isNull(0));
  }

  @Test
  public void testLoadStatusTableRendersUnreachableServer() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.isTrinoStarted()).thenReturn(true);
    when(manager.getLastLoadAttemptTimeMs()).thenReturn(2000L);
    when(manager.getLoadOutcome())
        .thenReturn(new CatalogConnectorManager.LoadOutcome(0L, "Connection refused", 3L));
    when(manager.getMetalakeErrors()).thenReturn(Map.of("test", "Connection refused"));

    Page page = new GravitinoSystemTableLoadStatus(manager, "test").loadPageData();

    // last_success_time stays null while the server is unreachable.
    assertTrue(page.getBlock(2).isNull(0));
    assertEquals(3, BIGINT.getLong(page.getBlock(3), 0));
    assertEquals("Connection refused", varchar(page, 4));
    assertEquals("{\"test\":\"Connection refused\"}", varchar(page, 5));
  }

  @Test
  public void testLoadStatusTableReportsOnlyItsOwnMetalakeErrors() {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.isTrinoStarted()).thenReturn(true);
    when(manager.getLastLoadAttemptTimeMs()).thenReturn(2000L);
    when(manager.getLoadOutcome())
        .thenReturn(new CatalogConnectorManager.LoadOutcome(0L, "1 of 2 metalakes failed", 1L));
    when(manager.getMetalakeErrors())
        .thenReturn(Map.of("test", "Connection refused", "dev", "Access Denied"));

    Page page = new GravitinoSystemTableLoadStatus(manager, "test").loadPageData();

    // The loop-wide fields describe the shared load loop, but metalake_errors is narrowed to the
    // metalake this entry catalog reports on.
    assertEquals("1 of 2 metalakes failed", varchar(page, 4));
    assertEquals("{\"test\":\"Connection refused\"}", varchar(page, 5));
  }

  @Test
  public void testEachFactoryOwnsItsTables() {
    // The registry used to be static, so a second connector in the same JVM took it over and the
    // system tables reported another manager's state. Each factory must now stand alone.
    CatalogConnectorManager first = mock(CatalogConnectorManager.class);
    when(first.getCatalogRegistrationStates("prod"))
        .thenReturn(
            List.of(
                CatalogRegistrationState.succeeded(
                    new GravitinoCatalog("prod", "memory", "memory", ImmutableMap.of(), 0L),
                    "memory")));
    CatalogConnectorManager second = mock(CatalogConnectorManager.class);
    when(second.getCatalogRegistrationStates("prod")).thenReturn(List.of());

    GravitinoSystemTableFactory firstFactory = new GravitinoSystemTableFactory(first, "prod");
    GravitinoSystemTableFactory secondFactory = new GravitinoSystemTableFactory(second, "prod");

    assertEquals(
        1,
        firstFactory.loadPageData(GravitinoSystemTableCatalogStatus.TABLE_NAME).getPositionCount());
    assertEquals(
        0,
        secondFactory
            .loadPageData(GravitinoSystemTableCatalogStatus.TABLE_NAME)
            .getPositionCount());
    assertTrue(firstFactory.tableExists(GravitinoSystemTableLoadStatus.TABLE_NAME));
    assertEquals(3, firstFactory.listTableNames().size());
  }

  private static Page loadCatalogStatusPage(List<CatalogRegistrationState> states) {
    CatalogConnectorManager manager = mock(CatalogConnectorManager.class);
    when(manager.getCatalogRegistrationStates("test")).thenReturn(states);
    return new GravitinoSystemTableCatalogStatus(manager, "test").loadPageData();
  }

  private static String varchar(Page page, int channel) {
    Block block = page.getBlock(channel);
    return VARCHAR.getSlice(block, 0).toStringUtf8();
  }
}
