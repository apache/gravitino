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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
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
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
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
public class SchemaAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";

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
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties);
    assertThrows(
        "Can not access metadata {" + CATALOG + "}.",
        RuntimeException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
        });
    // grant tester load catalog privilege
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    Catalog catalogEntity = gravitinoMetalake.loadCatalog(CATALOG);
    List<Privilege> privileges = new ArrayList<>();
    privileges.add(Privileges.UseCatalog.allow());
    SecurableObject securableObject = SecurableObjects.ofCatalog(catalogEntity.name(), privileges);
    securableObjects.add(securableObject);
    gravitinoMetalake.createRole(role, new HashMap<>(), securableObjects);
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
    Catalog catalogLoadByTester2 = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(CATALOG, catalogLoadByTester2.name());
  }

  @Test
  @Order(1)
  public void testCreateSchema() {
    // test catalog owner privilege
    Catalog catalogEntityLoadByTester1 = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    catalogEntityLoadByTester1.asSchemas().createSchema("schema1", "test", new HashMap<>());
    Catalog catalogEntityLoadByTester2 =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertThrows(
        "Can not access metadata {" + CATALOG + "}.",
        ForbiddenException.class,
        () -> {
          catalogEntityLoadByTester2.asSchemas().createSchema("schema2", "test2", new HashMap<>());
        });
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    // test grant create schema privilege
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(null, CATALOG, MetadataObject.Type.CATALOG),
        ImmutableList.of(Privileges.UseCatalog.allow(), Privileges.CreateSchema.allow()));
    catalogEntityLoadByTester2.asSchemas().createSchema("schema2", "test2", new HashMap<>());
    catalogEntityLoadByTester2.asSchemas().createSchema("schema3", "test2", new HashMap<>());
  }

  @Test
  @Order(2)
  public void testListSchema() {
    Catalog catalogEntityLoadByTester1 = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    String[] schemasLoadByTester1 = catalogEntityLoadByTester1.asSchemas().listSchemas();
    assertArrayEquals(
        new String[] {"default", "schema1", "schema2", "schema3"}, schemasLoadByTester1);
    Catalog catalogEntityLoadByTester2 =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    String[] schemasLoadByTester2 = catalogEntityLoadByTester2.asSchemas().listSchemas();
    assertArrayEquals(new String[] {"schema2", "schema3"}, schemasLoadByTester2);
  }

  @Test
  @Order(3)
  public void testLoadSchema() {
    Catalog catalogEntityLoadByTester2 =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    SupportsSchemas schemas = catalogEntityLoadByTester2.asSchemas();
    String[] schemasLoadByTester2 = schemas.listSchemas();
    assertArrayEquals(new String[] {"schema2", "schema3"}, schemasLoadByTester2);
    assertThrows(
        String.format("Can not access metadata {%s.%s}.", CATALOG, "schema1"),
        ForbiddenException.class,
        () -> {
          catalogEntityLoadByTester2.asSchemas().loadSchema("schema1");
        });
    // test grant use schema
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(CATALOG, "schema1", MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.UseSchema.allow()));
    schemasLoadByTester2 = schemas.listSchemas();
    assertArrayEquals(new String[] {"schema1", "schema2", "schema3"}, schemasLoadByTester2);
  }

  @Test
  @Order(4)
  public void testAlterSchema() {
    // test catalog owner privilege
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    Catalog catalogEntityLoadByTester1 = gravitinoMetalake.loadCatalog(CATALOG);
    catalogEntityLoadByTester1
        .asSchemas()
        .alterSchema("schema1", SchemaChange.setProperty("key", "value1"));
    catalogEntityLoadByTester1
        .asSchemas()
        .alterSchema("schema2", SchemaChange.setProperty("key2", "value2"));
    catalogEntityLoadByTester1
        .asSchemas()
        .alterSchema("schema3", SchemaChange.setProperty("key3", "value3"));
    Catalog catalogEntityLoadByTester2 =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertThrows(
        String.format("Can not access metadata {%s.%s}.", CATALOG, "schema1"),
        ForbiddenException.class,
        () -> {
          catalogEntityLoadByTester2
              .asSchemas()
              .alterSchema("schema1", SchemaChange.setProperty("key", "value"));
        });
    // test set owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, "schema1"), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    catalogEntityLoadByTester2
        .asSchemas()
        .alterSchema("schema1", SchemaChange.setProperty("key", "value"));
    catalogEntityLoadByTester2
        .asSchemas()
        .alterSchema("schema2", SchemaChange.setProperty("key", "value"));
  }

  @Test
  @Order(5)
  public void testDropSchema() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, "schema1"), MetadataObject.Type.SCHEMA),
        USER,
        Owner.Type.USER);
    Catalog catalogEntityLoadByTester2 =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertThrows(
        String.format("Can not access metadata {%s.%s}.", CATALOG, "schema1"),
        ForbiddenException.class,
        () -> {
          catalogEntityLoadByTester2.asSchemas().dropSchema("schema1", true);
        });
    catalogEntityLoadByTester2.asSchemas().dropSchema("schema2", true);
    Catalog catalogEntityLoadByTester1 = gravitinoMetalake.loadCatalog(CATALOG);
    catalogEntityLoadByTester1.asSchemas().dropSchema("schema1", true);
    catalogEntityLoadByTester1.asSchemas().dropSchema("schema3", true);
    String[] schemasLoadByTester1 = catalogEntityLoadByTester1.asSchemas().listSchemas();
    assertArrayEquals(new String[] {"default"}, schemasLoadByTester1);
    String[] schemasLoadByTester2 = catalogEntityLoadByTester2.asSchemas().listSchemas();
    assertArrayEquals(new String[] {}, schemasLoadByTester2);
    catalogEntityLoadByTester1.asSchemas().createSchema("schema1", "test", new HashMap<>());
    // test catalog owner
    assertThrows(
        String.format("Can not access metadata {%s.%s}.", CATALOG, "schema1"),
        ForbiddenException.class,
        () -> {
          catalogEntityLoadByTester2.asSchemas().dropSchema("schema1", true);
        });
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, "schema1"), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    catalogEntityLoadByTester2.asSchemas().dropSchema("schema1", true);
    schemasLoadByTester1 = catalogEntityLoadByTester1.asSchemas().listSchemas();
    assertArrayEquals(new String[] {"default"}, schemasLoadByTester1);
    schemasLoadByTester2 = catalogEntityLoadByTester2.asSchemas().listSchemas();
    assertArrayEquals(new String[] {}, schemasLoadByTester2);
  }
}
