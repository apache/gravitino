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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IcebergTableAuthorizationIT extends IcebergAuthorizationIT {

  private static final String SCHEMA_NAME = "schema";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());
  }

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    MetadataObject schemaObject =
        MetadataObjects.of(Arrays.asList(CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaObject, SUPER_USER, Owner.Type.USER);
    sql("USE rest;");
  }

  @Test
  void testCreateTable() {
    grantUseSchemaRole(SCHEMA_NAME);
    sql("USE %s;", SCHEMA_NAME);
    boolean exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, "abc"));
    Assertions.assertFalse(exists);
    Assertions.assertThrowsExactly(ForbiddenException.class, () -> sql("CREATE TABLE abc(a int)"));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, "abc"));
    Assertions.assertFalse(exists);

    grantCreateTableRole(SCHEMA_NAME);
    Assertions.assertDoesNotThrow(() -> sql("CREATE TABLE abc(a int)"));
    exists =
        catalogClientWithAllPrivilege
            .asTableCatalog()
            .tableExists(NameIdentifier.of(SCHEMA_NAME, "abc"));
    Assertions.assertTrue(exists);
  }

  private void grantUseSchemaRole(String schema) {
    String roleName = "useSchema_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE_NAME);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    gravitinoMetalake.createRole(roleName, new HashMap<>(), securableObjects);

    gravitinoMetalake.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  private void grantCreateTableRole(String schema) {
    String roleName = "createTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE_NAME);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject,
            schema,
            ImmutableList.of(Privileges.CreateTable.allow(), Privileges.SelectTable.allow()));
    securableObjects.add(schemaObject);
    gravitinoMetalake.createRole(roleName, new HashMap<>(), securableObjects);

    gravitinoMetalake.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);

    User user = gravitinoMetalake.getUser(NORMAL_USER);
    Assertions.assertNotNull(user);

    gravitinoMetalake.setOwner(
        MetadataObjects.of(Arrays.asList(CATALOG_NAME, schema), MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);
    Optional<Owner> owner =
        gravitinoMetalake.getOwner(
            MetadataObjects.of(Arrays.asList(CATALOG_NAME, schema), MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(owner.isPresent());
  }
}
