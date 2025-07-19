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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class OwnerAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static final String role = "role";
  private static final String TEMP_USER = "user3";

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
    client.loadMetalake(METALAKE).addUser(TEMP_USER);
    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table1"),
        new Column[] {Column.of("col1", Types.StringType.get())},
        "test",
        new HashMap<>());
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.loadMetalake(METALAKE).loadCatalog(CATALOG).asSchemas().dropSchema(SCHEMA, true);
    super.stopIntegrationTest();
  }

  @Test
  public void testSetTableOwnerByMetalakeOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        RuntimeException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
  }

  @Test
  public void testSetTableOwnerByCatalogOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        RuntimeException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set catalog owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        NORMAL_USER,
        Owner.Type.USER);
    // normal user can set owner
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG), MetadataObject.Type.CATALOG),
        USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }

  @Test
  public void testSetTableOwnerBySchemaOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        RuntimeException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set schema owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    // normal user can set owner
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }

  @Test
  public void testSetTableOwnerByTableOwner() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
    // normal user can not set owner
    GravitinoMetalake gravitinoMetalakeLoadByNormalUser = normalUserClient.loadMetalake(METALAKE);
    assertThrows(
        "Current user can not set owner",
        RuntimeException.class,
        () -> {
          gravitinoMetalakeLoadByNormalUser.setOwner(
              MetadataObjects.of(
                  ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
              NORMAL_USER,
              Owner.Type.USER);
        });
    // set schema owner to normal user
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);
    // normal user can set owner
    gravitinoMetalakeLoadByNormalUser.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        TEMP_USER,
        Owner.Type.USER);

    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        USER,
        Owner.Type.USER);
  }
}
