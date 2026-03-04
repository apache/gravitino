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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class MetadataObjectRoleAuthorizationIT extends BaseRestApiAuthorizationIT {

  private String hmsUri;

  private static final String CATALOG = "catalog1";

  @BeforeAll
  @Override
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
  }

  @Test
  public void testCatalogRole() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    Catalog catalog = gravitinoMetalake.loadCatalog(CATALOG);
    String[] roleNames = catalog.supportsRoles().listBindingRoleNames();
    assertArrayEquals(new String[] {}, roleNames);

    // create role1
    String role1 = "role1";
    gravitinoMetalake.createRole(
        role1,
        new HashMap<>(),
        ImmutableList.of(
            SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()))));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role1), NORMAL_USER);
    Catalog catalogLoadByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    roleNames = catalogLoadByNormalUser.supportsRoles().listBindingRoleNames();
    assertArrayEquals(new String[] {role1}, roleNames);

    // create role2
    String role2 = "role2";
    gravitinoMetalake.createRole(
        role2,
        new HashMap<>(),
        ImmutableList.of(
            SecurableObjects.ofCatalog(
                CATALOG, ImmutableList.of(Privileges.CreateSchema.allow()))));
    roleNames = catalogLoadByNormalUser.supportsRoles().listBindingRoleNames();
    assertArrayEquals(new String[] {role1}, roleNames);
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role2), NORMAL_USER);
    roleNames = catalogLoadByNormalUser.supportsRoles().listBindingRoleNames();
    Arrays.sort(roleNames);
    assertArrayEquals(new String[] {role1, role2}, roleNames);
    // reset
    gravitinoMetalake.deleteRole(role1);
    gravitinoMetalake.deleteRole(role2);
  }
}
