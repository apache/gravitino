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
package org.apache.gravitino.iceberg.integration.test.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TrinoIcebergRESTAuthorizationIT extends TrinoIcebergRESTAuthorizationITBase {

  private static final String ROLE = "trino_role";

  @BeforeAll
  public void setupTrino() throws Exception {
    catalogAsSuper.asSchemas().createSchema("db1", "", new HashMap<>());
    startTrino();
    sql(SUPER_CATALOG, "CREATE TABLE db1.shared (id integer)");
    sql(SUPER_CATALOG, "INSERT INTO db1.shared VALUES (1)");
  }

  @Test
  @Order(1)
  public void testNormalUserDeniedWithoutPrivileges() {
    assertThrows(RuntimeException.class, () -> sql(NORMAL_CATALOG, "SELECT * FROM db1.shared"));
  }

  @Test
  @Order(2)
  public void testNormalUserAllowedAfterGrant() {
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            CATALOG_NAME,
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow()));
    metalakeAsSuper.createRole(ROLE, new HashMap<>(), ImmutableList.of(catalogObject));
    metalakeAsSuper.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);

    assertEquals(1L, sql(NORMAL_CATALOG, "SELECT count(*) FROM db1.shared").getOnlyValue());
  }

  @Test
  @Order(3)
  public void testNormalUserDeniedCreateTable() {
    assertThrows(
        RuntimeException.class,
        () -> sql(NORMAL_CATALOG, "CREATE TABLE db1.normal_made (id integer)"));
  }
}
