/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.integration.test.rest;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogIT extends BaseRestApiIT {

  protected ContainerSuite containerSuite = ContainerSuite.getInstance();

  private String catalog1 = "catalog1";

  private String catalog2 = "catalog2";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
  }

  @Test
  @Order(1)
  public void testCreateCatalog() {
    assertThrows(
        "Can not access metadata.",
        RuntimeException.class,
        () -> {
          clientWithNoAuthorization
              .loadMetalake(METALAKE)
              .createCatalog(
                  catalog1,
                  Catalog.Type.RELATIONAL,
                  "test",
                  "comment",
                  ImmutableMap.of("key", "value"));
        });
    client
        .loadMetalake(METALAKE)
        .createCatalog(
            catalog1, Catalog.Type.RELATIONAL, "test", "comment", ImmutableMap.of("key", "value"));
    client
        .loadMetalake(METALAKE)
        .createCatalog(
            catalog2, Catalog.Type.RELATIONAL, "test", "comment", ImmutableMap.of("key", "value"));
  }

  @Test
  @Order(2)
  public void testListCatalog() {
    String[] catalogs = clientWithNoAuthorization.loadMetalake(METALAKE).listCatalogs();
    assertEquals(0, catalogs.length);
    catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(2, catalogs.length);
    assertArrayEquals(new String[] {catalog1, catalog2}, catalogs);
  }

  @Test
  @Order(3)
  public void testDeleteCatalog() {
    String[] catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(2, catalogs.length);
    assertArrayEquals(new String[] {catalog1, catalog2}, catalogs);
    assertThrows(
        "Can not access metadata.",
        RuntimeException.class,
        () -> {
          clientWithNoAuthorization.loadMetalake(METALAKE).dropCatalog(catalog1, true);
        });
    client.loadMetalake(METALAKE).dropCatalog(catalog1, true);
    catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(1, catalogs.length);
    assertArrayEquals(new String[] {catalog2}, catalogs);
    client.loadMetalake(METALAKE).dropCatalog(catalog2, true);
    catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(0, catalogs.length);
  }
}
