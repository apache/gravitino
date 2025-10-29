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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PartitionAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static String role = "role";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    client
        .loadMetalake(METALAKE)
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties)
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());
    // try to load the schema as normal user, expect failure
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        RuntimeException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asSchemas()
              .loadSchema(SCHEMA);
        });
    // grant tester privilege
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    gravitinoMetalake.createRole(role, new HashMap<>(), securableObjects);
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
    // normal user can load the catalog but not the schema
    Catalog catalogLoadByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(CATALOG, catalogLoadByNormalUser.name());
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          catalogLoadByNormalUser.asSchemas().loadSchema(SCHEMA);
        });
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table1"), createColumns(), "test", new HashMap<>());
    // normal user cannot create table
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.createTable(
              NameIdentifier.of(SCHEMA, "table2"), createColumns(), "test2", new HashMap<>());
        });
  }

  private Column[] createColumns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }

  @Test
  @Order(1)
  public void testCreatePartition() {
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    Table table1 = tableCatalog.loadTable(NameIdentifier.of(SCHEMA, "table1"));

    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    SupportsPartitions supportsPartitions =
        tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1")).supportPartitions();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          supportsPartitions.addPartition(
              Partitions.list(
                  "p1",
                  new Literal[][] {
                    {
                      Literals.stringLiteral("1"),
                    },
                    {Literals.stringLiteral("2")}
                  },
                  Maps.newHashMap()));
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.addPartition(
        Partitions.list(
            "p1",
            new Literal[][] {
              {
                Literals.stringLiteral("1"),
              },
              {Literals.stringLiteral("2")}
            },
            Maps.newHashMap()));
  }

  @Test
  @Order(2)
  public void testListPartition() {
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    Table table1 = tableCatalog.loadTable(NameIdentifier.of(SCHEMA, "table1"));

    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    SupportsPartitions supportsPartitions =
        tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1")).supportPartitions();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        supportsPartitions::listPartitions);
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.listPartitions();
  }

  @Test
  @Order(3)
  public void testLoadPartition() {
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    Table table1 = tableCatalog.loadTable(NameIdentifier.of(SCHEMA, "table1"));

    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    SupportsPartitions supportsPartitions =
        tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1")).supportPartitions();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          supportsPartitions.getPartition("p1");
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.getPartition("p1");
  }

  @Test
  @Order(4)
  public void testDropPartition() {
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    Table table1 = tableCatalog.loadTable(NameIdentifier.of(SCHEMA, "table1"));

    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    SupportsPartitions supportsPartitions =
        tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1")).supportPartitions();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          supportsPartitions.dropPartition("p1");
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.dropPartition("p1");
  }
}
