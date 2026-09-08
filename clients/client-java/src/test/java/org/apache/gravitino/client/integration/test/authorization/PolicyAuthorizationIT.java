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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PolicyAuthorizationIT extends BaseRestApiAuthorizationIT {

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
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
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
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          catalogLoadByNormalUser.asSchemas().loadSchema(SCHEMA);
        });
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table1"), createColumns(), "test", new HashMap<>());
  }

  private Column[] createColumns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    gravitinoMetalake.dropCatalog(CATALOG, true);
    super.stopIntegrationTest();
  }

  @Test
  @Order(1)
  public void createPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    Set<MetadataObject.Type> supportedTypes = ImmutableSet.of(MetadataObject.Type.TABLE);
    gravitinoMetalake.createPolicy(
        "policy1", "custom", "policy1", true, PolicyContents.custom(null, supportedTypes, null));
    GravitinoMetalake metalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          metalakeLoadByNormalUser.createPolicy(
              "policy2",
              "custom",
              "policy2",
              true,
              PolicyContents.custom(null, supportedTypes, null));
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableSet.of(Privileges.CreatePolicy.allow()));
    metalakeLoadByNormalUser.createPolicy(
        "policy2", "custom", "policy2", true, PolicyContents.custom(null, supportedTypes, null));
    gravitinoMetalake.createPolicy(
        "policy3", "custom", "policy3", true, PolicyContents.custom(null, supportedTypes, null));
  }

  @Test
  @Order(2)
  public void listPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    String[] policies = gravitinoMetalake.listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy1", "policy2", "policy3"}, policies);
    policies = normalUserClient.loadMetalake(METALAKE).listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2"}, policies);
  }

  @Test
  @Order(3)
  public void testGetPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getPolicy("policy1");
    gravitinoMetalake.getPolicy("policy2");
    gravitinoMetalake.getPolicy("policy3");
    GravitinoMetalake metalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          metalakeLoadByNormalUser.getPolicy("policy1");
        });
    metalakeLoadByNormalUser.getPolicy("policy2");
    assertThrows(
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          metalakeLoadByNormalUser.getPolicy("policy3");
        });

    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of("policy3"), MetadataObject.Type.POLICY),
        ImmutableSet.of(Privileges.ApplyPolicy.allow()));
    metalakeLoadByNormalUser.getPolicy("policy3");
    String[] policies = metalakeLoadByNormalUser.listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2", "policy3"}, policies);
  }

  @Test
  @Order(4)
  public void testAlterPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake metalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalake.alterPolicy("policy1", PolicyChange.updateComment("222"));
    assertThrows(
        "Can not access metadata.{" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          metalakeLoadByNormalUser.alterPolicy("policy3", PolicyChange.updateComment("222"));
        });
    metalakeLoadByNormalUser.alterPolicy("policy2", PolicyChange.updateComment("222"));
  }

  @Test
  @Order(6)
  public void testAssociatePolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake metalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    SupportsPolicies supportsPolicies =
        gravitinoMetalake
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsPolicies();
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.SelectTable.allow(), Privileges.UseSchema.allow()));
    SupportsPolicies supportsPoliciesByNormalUser =
        metalakeLoadByNormalUser
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsPolicies();
    supportsPolicies.associatePolicies(new String[] {"policy1"}, new String[] {});
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          supportsPoliciesByNormalUser.associatePolicies(new String[] {"policy1"}, new String[] {});
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableSet.of(Privileges.CreateTable.allow()));
    TableCatalog tableCatalog =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table2"), createColumns(), "test", new HashMap<>());
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table3"), createColumns(), "test", new HashMap<>());
    metalakeLoadByNormalUser
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table2"))
        .supportsPolicies()
        .associatePolicies(new String[] {"policy2"}, new String[] {});

    metalakeLoadByNormalUser
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table3"))
        .supportsPolicies()
        .associatePolicies(new String[] {"policy2"}, new String[] {});
  }

  @Test
  @Order(7)
  public void testListPolicyForTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    String[] policies =
        gravitinoMetalake
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy1"}, policies);
    policies =
        gravitinoMetalake
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table2"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2"}, policies);
    policies =
        gravitinoMetalake
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table3"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2"}, policies);
    policies =
        gravitinoMetalakeLoadByNormalUser
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {}, policies);
    policies =
        gravitinoMetalakeLoadByNormalUser
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table2"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2"}, policies);
    policies =
        gravitinoMetalakeLoadByNormalUser
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table3"))
            .supportsPolicies()
            .listPolicies();
    Assertions.assertArrayEquals(new String[] {"policy2"}, policies);
  }

  @Test
  @Order(8)
  public void testGetPolicyForTable() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalake
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table1"))
        .supportsPolicies()
        .getPolicy("policy1");
    gravitinoMetalake
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table2"))
        .supportsPolicies()
        .getPolicy("policy2");
    gravitinoMetalake
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table3"))
        .supportsPolicies()
        .getPolicy("policy2");
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser
              .loadCatalog(CATALOG)
              .asTableCatalog()
              .loadTable(NameIdentifier.of(SCHEMA, "table1"))
              .supportsPolicies()
              .getPolicy("policy1");
        });

    gravitinoMetalakeLoadByNormalUser
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table2"))
        .supportsPolicies()
        .getPolicy("policy2");
    gravitinoMetalakeLoadByNormalUser
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table3"))
        .supportsPolicies()
        .getPolicy("policy2");
  }

  @Test
  @Order(9)
  public void testListObjForPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    String[] tables =
        Arrays.stream(
                gravitinoMetalakeLoadByNormalUser
                    .getPolicy("policy2")
                    .associatedObjects()
                    .objects())
            .map(MetadataObject::name)
            .toList()
            .toArray(new String[0]);
    Arrays.sort(tables);
    Assertions.assertArrayEquals(new String[] {"table2", "table3"}, tables);
    MetadataObject metadataObject =
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table2"), MetadataObject.Type.TABLE);
    gravitinoMetalake.setOwner(metadataObject, USER, Owner.Type.USER);
    gravitinoMetalake.grantPrivilegesToRole(
        role, metadataObject, ImmutableSet.of(Privileges.SelectTable.deny()));
    tables =
        Arrays.stream(
                gravitinoMetalakeLoadByNormalUser
                    .getPolicy("policy2")
                    .associatedObjects()
                    .objects())
            .map(MetadataObject::name)
            .toList()
            .toArray(new String[0]);
    Arrays.sort(tables);
    Assertions.assertArrayEquals(new String[] {"table3"}, tables);
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getPolicy("policy1").associatedObjects().objects();
        });
    tables =
        Arrays.stream(gravitinoMetalake.getPolicy("policy1").associatedObjects().objects())
            .map(MetadataObject::name)
            .toList()
            .toArray(new String[0]);
    Arrays.sort(tables);
    Assertions.assertArrayEquals(new String[] {"table1"}, tables);
  }

  @Test
  @Order(10)
  public void testDropPolicy() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    GravitinoMetalake metalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          metalakeLoadByNormalUser.deletePolicy("policy1");
        });
    gravitinoMetalake.deletePolicy("policy1");
    metalakeLoadByNormalUser.deletePolicy("policy2");
  }
}
