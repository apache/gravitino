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
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TableAuthorizationIT extends BaseRestApiAuthorizationIT {

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
  }

  private Column[] createColumns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }

  @Test
  @Order(1)
  public void testCreateTable() {
    // owner can create table
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
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.listTables(Namespace.of(SCHEMA));
        });
    // grant privileges
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateTable.allow()));
    // normal user can now create table
    tableCatalogNormalUser.createTable(
        NameIdentifier.of(SCHEMA, "table2"), createColumns(), "test2", new HashMap<>());
    tableCatalogNormalUser.createTable(
        NameIdentifier.of(SCHEMA, "table3"), createColumns(), "test2", new HashMap<>());
    String tempUser = "tempUser";
    gravitinoMetalake.addUser(tempUser);
    GravitinoAdminClient tempUserClient =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(tempUser).build();
    String tempRole = "tempRole";
    gravitinoMetalake.createRole(tempRole, new HashMap<>(), new ArrayList<>());
    gravitinoMetalake.grantPrivilegesToRole(
        tempRole,
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseCatalog.allow()));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(tempRole), tempUser);
    TableCatalog catalogLoadByTmpUser =
        tempUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          catalogLoadByTmpUser.createTable(
              NameIdentifier.of(SCHEMA, "table2"), createColumns(), "test2", new HashMap<>());
        });
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), tempUser);
    catalogLoadByTmpUser.createTable(
        NameIdentifier.of(SCHEMA, "table4"), createColumns(), "test2", new HashMap<>());
    tableCatalog.dropTable(NameIdentifier.of(SCHEMA, "table4"));
  }

  @Test
  @Order(2)
  public void testListTable() {
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    NameIdentifier[] tablesList = tableCatalog.listTables(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "table1"),
          NameIdentifier.of(SCHEMA, "table2"),
          NameIdentifier.of(SCHEMA, "table3")
        },
        tablesList);
    // normal user can only see tables they have privilege for
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    NameIdentifier[] tablesListNormalUser = tableCatalogNormalUser.listTables(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "table2"), NameIdentifier.of(SCHEMA, "table3")
        },
        tablesListNormalUser);
  }

  @Test
  @Order(3)
  public void testLoadTable() {
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    // normal user can load table2 and table3, but not table1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
        });
    Table table2 = tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table2"));
    assertEquals("table2", table2.name());
    Table table3 = tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table3"));
    assertEquals("table3", table3.name());

    // grant normal user privilege to use table1
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.SelectTable.allow()));
    tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        ImmutableList.of(Privileges.SelectTable.deny()));
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
        });
  }

  @Test
  @Order(4)
  public void testAlterTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();

    // normal user cannot alter table1 (no privilege)
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.alterTable(
              NameIdentifier.of(SCHEMA, "table1"), TableChange.setProperty("key", "value"));
        });
    // grant normal user owner privilege on table1
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    tableCatalogNormalUser.alterTable(
        NameIdentifier.of(SCHEMA, "table1"), TableChange.setProperty("key", "value"));
  }

  @Test
  @Order(5)
  public void testDropTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user cannot drop table1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.dropTable(NameIdentifier.of(SCHEMA, "table1"));
        });
    // normal user can drop table2 and table3 (they created them)
    tableCatalogNormalUser.dropTable(NameIdentifier.of(SCHEMA, "table2"));
    tableCatalogNormalUser.dropTable(NameIdentifier.of(SCHEMA, "table3"));

    // owner can drop table1
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.dropTable(NameIdentifier.of(SCHEMA, "table1"));
    // check tables are dropped
    NameIdentifier[] tablesList = tableCatalog.listTables(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, tablesList);
    NameIdentifier[] tablesListNormalUser = tableCatalogNormalUser.listTables(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, tablesListNormalUser);
  }
}
