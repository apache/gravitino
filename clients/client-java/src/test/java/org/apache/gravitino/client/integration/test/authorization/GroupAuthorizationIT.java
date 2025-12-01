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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GroupAuthorizationIT extends BaseRestApiAuthorizationIT {

  @Test
  @Order(1)
  public void testCreateGroup() {
    assertThrows(
        "Current user access metadata {testMetalake}",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).addGroup("group1");
        });
    client.loadMetalake(METALAKE).addGroup("group1");
    client.loadMetalake(METALAKE).addGroup("group2");
  }

  @Test
  @Order(2)
  public void testRemoveGroup() {
    assertThrows(
        "Current user access metadata {testMetalake}",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).removeGroup("group1");
        });
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    // owner can remove user
    gravitinoMetalake.removeGroup("group1");
    // user1 can remove group after grant
    gravitinoMetalake.createRole("role", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.grantPrivilegesToRole(
        "role",
        MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.ManageGroups.allow()));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of("role"), NORMAL_USER);
    normalUserClient.loadMetalake(METALAKE).removeGroup("group2");
  }

  @Test
  @Order(3)
  public void testListGroupsWithNonExistentMetalake() throws Exception {
    // Test that listGroups with @AuthorizationExpression returns 403 Forbidden
    // when the metalake doesn't exist, instead of 404 response
    String nonExistentMetalake = "nonExistentMetalake";

    // Access the restClient from normalUserClient using reflection
    Method restClientMethod =
        normalUserClient.getClass().getSuperclass().getDeclaredMethod("restClient");
    restClientMethod.setAccessible(true);
    Object restClient = restClientMethod.invoke(normalUserClient);

    // Create a MetalakeDTO for the non-existent metalake
    MetalakeDTO metalakeDTO =
        MetalakeDTO.builder()
            .withName(nonExistentMetalake)
            .withComment("test")
            .withProperties(Maps.newHashMap())
            .withAudit(
                org.apache.gravitino.dto.AuditDTO.builder()
                    .withCreator("test")
                    .withCreateTime(java.time.Instant.now())
                    .build())
            .build();

    // Use DTOConverters.toMetaLake() via reflection to create GravitinoMetalake
    Class<?> dtoConvertersClass = Class.forName("org.apache.gravitino.client.DTOConverters");
    Method toMetaLakeMethod =
        dtoConvertersClass.getDeclaredMethod(
            "toMetaLake",
            MetalakeDTO.class,
            Class.forName("org.apache.gravitino.client.RESTClient"));
    toMetaLakeMethod.setAccessible(true);
    GravitinoMetalake nonExistentMetalakeObj =
        (GravitinoMetalake) toMetaLakeMethod.invoke(null, metalakeDTO, restClient);

    // Test listGroups - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, nonExistentMetalakeObj::listGroups);

    // Test listGroupNames - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, nonExistentMetalakeObj::listGroupNames);
  }
}
