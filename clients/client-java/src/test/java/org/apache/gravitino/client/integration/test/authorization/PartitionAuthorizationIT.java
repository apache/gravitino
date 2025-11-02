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

import static org.apache.gravitino.client.integration.test.StatisticIT.TABLE_COMMENT;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
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
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
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
  private static final String SCHEMA = "partitionSchema";
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
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        catalogObject,
        ImmutableSet.of(
            Privileges.SelectTable.allow(),
            Privileges.UseSchema.allow(),
            Privileges.UseCatalog.allow()));
    // normal user can load the catalog but not the schema
    Catalog catalogLoadByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(CATALOG, catalogLoadByNormalUser.name());
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    Column[] columns = createColumns();

    NameIdentifier nameIdentifier = NameIdentifier.of(SCHEMA, "table1");

    tableCatalog.createTable(
        nameIdentifier,
        columns,
        TABLE_COMMENT,
        new HashMap<>(),
        new Transform[] {Transforms.identity(columns[0].name())});
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

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.loadMetalake(METALAKE).loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    super.stopIntegrationTest();
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
    SupportsPartitions supportsPartitions = getTable1(tableCatalogNormalUser).supportPartitions();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          supportsPartitions.addPartition(genPartition());
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.addPartition(genPartition());
  }

  private static Table getTable1(TableCatalog tableCatalogNormalUser) {
    return tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
  }

  private static IdentityPartition genPartition() {
    return Partitions.identity(
        new String[][] {{"col1"}}, new Literal[] {Literals.stringLiteral("1")});
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
    MetadataObject metadataObject = MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG);
    client
        .loadMetalake(METALAKE)
        .revokePrivilegesFromRole(
            role, metadataObject, ImmutableSet.of(Privileges.SelectTable.allow()));
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        supportsPartitions::listPartitions);
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.listPartitions();
    client
        .loadMetalake(METALAKE)
        .grantPrivilegesToRole(
            role, metadataObject, ImmutableSet.of(Privileges.SelectTable.allow()));
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
    MetadataObject metadataObject = MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG);
    client
        .loadMetalake(METALAKE)
        .revokePrivilegesFromRole(
            role, metadataObject, ImmutableSet.of(Privileges.SelectTable.allow()));
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          supportsPartitions.getPartition("col1=1");
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.getPartition("col1=1");
    client
        .loadMetalake(METALAKE)
        .grantPrivilegesToRole(
            role, metadataObject, ImmutableSet.of(Privileges.SelectTable.allow()));
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
          supportsPartitions.dropPartition("col1=1");
        });
    SupportsPartitions supportsPartitionsByAdmin = table1.supportPartitions();
    supportsPartitionsByAdmin.dropPartition("col1=1");
  }
}
