/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class IcebergNamespaceAuthorizationIT extends IcebergAuthorizationIT {

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
  }

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    grantUseCatalogRole(GRAVITINO_CATALOG_NAME);
    sql("USE rest;");
  }

  @Test
  void testCreateSchemaWithOwner() {
    String namespace = "ns_owner";
    sql("CREATE DATABASE %s", namespace);
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, namespace), MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());

    // test that the owner has privileges to create a table on the schema
    String tableName = "table_owner";
    sql("USE %s", namespace);
    sql("CREATE TABLE %s(a int)", tableName);
    sql("DESC TABLE %s", tableName);
    owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, namespace, tableName),
                MetadataObject.Type.TABLE));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());
  }

  private void grantUseCatalogRole(String catalogName) {
    String roleName = "useCatalog_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }
}
