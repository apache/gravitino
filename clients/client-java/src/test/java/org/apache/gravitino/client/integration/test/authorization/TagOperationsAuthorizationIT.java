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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.TagChange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TagOperationsAuthorizationIT extends BaseRestApiAuthorizationIT {

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

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    gravitinoMetalake.dropCatalog(CATALOG, true);
    super.stopIntegrationTest();
  }

  @Test
  @Order(1)
  public void createTag() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.createTag("tag1", "tag1", Map.of());
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).createTag("tag2", "tag2", Map.of());
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
        ImmutableSet.of(Privileges.CreateTag.allow()));
    normalUserClient.loadMetalake(METALAKE).createTag("tag2", "tag2", Map.of());
    client.loadMetalake(METALAKE).createTag("tag3", "tag3", Map.of());
  }

  @Test
  @Order(2)
  public void testAlterTag() {
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.alterTag("tag1", TagChange.setProperty("k1", "v1"));
        });
    gravitinoMetalakeLoadByNormalUser.alterTag("tag2", TagChange.setProperty("k1", "v1"));
  }

  @Test
  @Order(3)
  public void testLoadTag() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    String[] tagsLoadByAdmin = gravitinoMetalake.listTags();
    assertArrayEquals(new String[] {"tag1", "tag2", "tag3"}, tagsLoadByAdmin);
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    String[] tagsLoadByNormalUser = gravitinoMetalakeLoadByNormalUser.listTags();
    assertArrayEquals(new String[] {"tag2"}, tagsLoadByNormalUser);
  }

  @Test
  @Order(4)
  public void testGetTag() {
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    gravitinoMetalakeLoadByNormalUser.getTag("tag2");
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.getTag("tag1");
        });
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.getTag("tag1");
    gravitinoMetalake.getTag("tag2");
    gravitinoMetalake.getTag("tag3");
  }

  @Test
  @Order(6)
  public void testAssociateTag() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    SupportsTags supportsTags = gravitinoMetalake.loadCatalog(CATALOG).supportsTags();
    supportsTags.associateTags(new String[] {"tag1"}, null);
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asTableCatalog()
              .loadTable(NameIdentifier.of(SCHEMA, "table1"))
              .supportsTags()
              .associateTags(new String[] {"tag1"}, null);
        });
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asTableCatalog()
              .loadTable(NameIdentifier.of(SCHEMA, "table1"))
              .supportsTags()
              .associateTags(new String[] {"tag2"}, null);
        });
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.SelectTable.allow(), Privileges.UseSchema.allow()));
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asTableCatalog()
              .loadTable(NameIdentifier.of(SCHEMA, "table1"))
              .supportsTags()
              .associateTags(new String[] {"tag1"}, null);
        });
    normalUserClient
        .loadMetalake(METALAKE)
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table1"))
        .supportsTags()
        .associateTags(new String[] {"tag2"}, null);
    client
        .loadMetalake(METALAKE)
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table1"))
        .supportsTags()
        .associateTags(new String[] {"tag1"}, null);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of("tag3"), MetadataObject.Type.TAG),
        ImmutableList.of(Privileges.ApplyTag.allow()));
    normalUserClient
        .loadMetalake(METALAKE)
        .loadCatalog(CATALOG)
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA, "table1"))
        .supportsTags()
        .associateTags(new String[] {"tag3"}, null);
  }

  @Test
  @Order(7)
  public void testListTagByMetadata() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    SupportsTags tableSupportTag =
        gravitinoMetalake
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsTags();
    String[] tagsLoadByAdmin = tableSupportTag.listTags();
    Arrays.sort(tagsLoadByAdmin);
    assertArrayEquals(new String[] {"tag1", "tag2", "tag3"}, tagsLoadByAdmin);
    GravitinoMetalake gravitinoMetalakeByNormalUser = normalUserClient.loadMetalake(METALAKE);
    SupportsTags tableSupportTagByNormalUser =
        gravitinoMetalakeByNormalUser
            .loadCatalog(CATALOG)
            .asTableCatalog()
            .loadTable(NameIdentifier.of(SCHEMA, "table1"))
            .supportsTags();
    String[] tagsLoadByNormalUser = tableSupportTagByNormalUser.listTags();
    Arrays.sort(tagsLoadByNormalUser);
    assertArrayEquals(new String[] {"tag2", "tag3"}, tagsLoadByNormalUser);
  }

  @Test
  @Order(8)
  public void testDropTag() {
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Can not access metadata.",
        ForbiddenException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.deleteTag("tag1");
        });
    gravitinoMetalakeLoadByNormalUser.deleteTag("tag2");
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.deleteTag("tag1");
  }

  private Column[] createColumns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }
}
