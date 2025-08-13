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

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogAuthorizationIT extends BaseRestApiAuthorizationIT {

  private String catalog1 = "catalog1";

  private String catalog2 = "catalog2";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static String hmsUri;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
  }

  @Test
  @Order(1)
  public void testCreateCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    assertThrows(
        "Can not access metadata {" + catalog1 + "}.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createCatalog(catalog1, Catalog.Type.RELATIONAL, "hive", "comment", properties);
        });
    client
        .loadMetalake(METALAKE)
        .createCatalog(catalog1, Catalog.Type.RELATIONAL, "hive", "comment", properties);
    client
        .loadMetalake(METALAKE)
        .createCatalog(catalog2, Catalog.Type.RELATIONAL, "hive", "comment", properties);
    assertThrows(
        "Can not access metadata {" + catalog1 + "}.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createCatalog(catalog1, Catalog.Type.RELATIONAL, "hive", "comment", properties);
        });
  }

  @Test
  @Order(2)
  public void testListCatalog() {
    String[] catalogs = normalUserClient.loadMetalake(METALAKE).listCatalogs();
    assertEquals(0, catalogs.length);
    catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(2, catalogs.length);
    assertArrayEquals(new String[] {catalog1, catalog2}, catalogs);
  }

  @Test
  @Order(2)
  public void testListCatalogInfo() {
    Catalog[] catalogs = normalUserClient.loadMetalake(METALAKE).listCatalogsInfo();
    assertCatalogEquals(new String[] {}, catalogs);
    catalogs = client.loadMetalake(METALAKE).listCatalogsInfo();
    assertCatalogEquals(new String[] {catalog1, catalog2}, catalogs);
  }

  private void assertCatalogEquals(String[] expectCatalogs, Catalog[] actualCatalogs) {
    assertEquals(expectCatalogs.length, actualCatalogs.length);
    for (int i = 0; i < expectCatalogs.length; i++) {
      assertEquals(expectCatalogs[i], actualCatalogs[i].name());
    }
  }

  @Test
  @Order(3)
  public void testDeleteCatalog() {
    String[] catalogs = client.loadMetalake(METALAKE).listCatalogs();
    assertEquals(2, catalogs.length);
    assertArrayEquals(new String[] {catalog1, catalog2}, catalogs);
    assertThrows(
        "Can not access metadata {" + catalog1 + "}.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).dropCatalog(catalog1, true);
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
