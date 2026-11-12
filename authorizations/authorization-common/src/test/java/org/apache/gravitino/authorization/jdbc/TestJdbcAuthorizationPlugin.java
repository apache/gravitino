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
package org.apache.gravitino.authorization.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcAuthorizationPlugin {
  private static List<String> expectSQLs = Lists.newArrayList();
  private static List<MetadataObject.Type> expectTypes = Lists.newArrayList();
  private static List<String> expectObjectNames = Lists.newArrayList();
  private static List<Optional<Owner>> expectPreOwners = Lists.newArrayList();
  private static List<Owner> expectNewOwners = Lists.newArrayList();
  private static int currentSQLIndex = 0;
  private static int currentIndex = 0;
  private static final Map<String, String> properties =
      ImmutableMap.of(
          JdbcAuthorizationProperties.JDBC_URL,
          "jdbc:h2:mem:test",
          JdbcAuthorizationProperties.JDBC_USERNAME,
          "xx",
          JdbcAuthorizationProperties.JDBC_PASSWORD,
          "xx",
          JdbcAuthorizationProperties.JDBC_DRIVER,
          "org.h2.Driver");

  private static final JdbcAuthorizationPlugin plugin =
      new JdbcAuthorizationPlugin(properties) {

        @Override
        public List<String> getSetOwnerSQL(
            MetadataObject.Type type, String objectName, Owner preOwner, Owner newOwner) {
          Assertions.assertEquals(expectTypes.get(currentIndex), type);
          Assertions.assertEquals(expectObjectNames.get(currentIndex), objectName);
          Assertions.assertEquals(expectPreOwners.get(currentIndex), Optional.ofNullable(preOwner));
          Assertions.assertEquals(expectNewOwners.get(currentIndex), newOwner);
          currentIndex++;
          return Collections.emptyList();
        }

        public boolean executeUpdateSQL(String sql, String ignoreErrorMsg) {
          Assertions.assertEquals(expectSQLs.get(currentSQLIndex), sql);
          currentSQLIndex++;
          return true;
        }
      };

  @Test
  public void testUserManagement() {
    expectSQLs = Lists.newArrayList("CREATE USER tmp");
    currentSQLIndex = 0;
    plugin.onUserAdded(createUser("tmp"));

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> plugin.onUserAcquired(createUser("tmp")));

    expectSQLs = Lists.newArrayList("DROP USER tmp");
    currentSQLIndex = 0;
    plugin.onUserRemoved(createUser("tmp"));
  }

  @Test
  public void testGroupManagement() {
    expectSQLs = Lists.newArrayList("CREATE USER GRAVITINO_GROUP_tmp");
    resetSQLIndex();
    plugin.onGroupAdded(createGroup("tmp"));

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> plugin.onGroupAcquired(createGroup("tmp")));

    expectSQLs = Lists.newArrayList("DROP USER GRAVITINO_GROUP_tmp");
    resetSQLIndex();
    plugin.onGroupRemoved(createGroup("tmp"));
  }

  @Test
  public void testRoleManagement() {
    expectSQLs = Lists.newArrayList("CREATE ROLE tmp");
    resetSQLIndex();
    Role role = createRole("tmp");
    plugin.onRoleCreated(role);

    Assertions.assertThrows(UnsupportedOperationException.class, () -> plugin.onRoleAcquired(role));

    resetSQLIndex();
    expectSQLs = Lists.newArrayList("DROP ROLE tmp");
    Assertions.assertTrue(plugin.onRoleDeleted(role));
  }

  @Test
  public void testPermissionManagement() {
    Role role = createRole("tmp");
    Group group = createGroup("tmp");
    User user = createUser("tmp");

    resetSQLIndex();
    expectSQLs =
        Lists.newArrayList("CREATE ROLE tmp", "GRANT ROLE tmp TO USER GRAVITINO_GROUP_tmp");
    plugin.onGrantedRolesToGroup(Lists.newArrayList(role), group);

    resetSQLIndex();
    expectSQLs = Lists.newArrayList("CREATE ROLE tmp", "GRANT ROLE tmp TO USER tmp");
    plugin.onGrantedRolesToUser(Lists.newArrayList(role), user);

    resetSQLIndex();
    expectSQLs =
        Lists.newArrayList("CREATE ROLE tmp", "REVOKE ROLE tmp FROM USER GRAVITINO_GROUP_tmp");
    plugin.onRevokedRolesFromGroup(Lists.newArrayList(role), group);

    resetSQLIndex();
    expectSQLs = Lists.newArrayList("CREATE ROLE tmp", "REVOKE ROLE tmp FROM USER tmp");
    plugin.onRevokedRolesFromUser(Lists.newArrayList(role), user);

    // Test metalake object and different role change
    resetSQLIndex();
    expectSQLs = Lists.newArrayList("GRANT SELECT ON TABLE *.* TO ROLE tmp");
    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake("metalake", Lists.newArrayList(Privileges.SelectTable.allow()));
    RoleChange roleChange = RoleChange.addSecurableObject("tmp", metalakeObject);
    plugin.onRoleUpdated(role, roleChange);

    resetSQLIndex();
    expectSQLs = Lists.newArrayList("REVOKE SELECT ON TABLE *.* FROM ROLE tmp");
    roleChange = RoleChange.removeSecurableObject("tmp", metalakeObject);
    plugin.onRoleUpdated(role, roleChange);

    resetSQLIndex();
    expectSQLs =
        Lists.newArrayList(
            "REVOKE SELECT ON TABLE *.* FROM ROLE tmp", "GRANT CREATE ON TABLE *.* TO ROLE tmp");
    SecurableObject newMetalakeObject =
        SecurableObjects.ofMetalake("metalake", Lists.newArrayList(Privileges.CreateTable.allow()));
    roleChange = RoleChange.updateSecurableObject("tmp", metalakeObject, newMetalakeObject);
    plugin.onRoleUpdated(role, roleChange);

    // Test catalog object
    resetSQLIndex();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.SelectTable.allow()));
    roleChange = RoleChange.addSecurableObject("tmp", catalogObject);
    expectSQLs = Lists.newArrayList("GRANT SELECT ON TABLE *.* TO ROLE tmp");
    plugin.onRoleUpdated(role, roleChange);

    // Test schema object
    resetSQLIndex();
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, "schema", Lists.newArrayList(Privileges.SelectTable.allow()));
    roleChange = RoleChange.addSecurableObject("tmp", schemaObject);
    expectSQLs = Lists.newArrayList("GRANT SELECT ON TABLE schema.* TO ROLE tmp");
    plugin.onRoleUpdated(role, roleChange);

    // Test table object
    resetSQLIndex();
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "table", Lists.newArrayList(Privileges.SelectTable.allow()));
    roleChange = RoleChange.addSecurableObject("tmp", tableObject);
    expectSQLs = Lists.newArrayList("GRANT SELECT ON TABLE schema.table TO ROLE tmp");
    plugin.onRoleUpdated(role, roleChange);

    // Test the role with objects
    resetSQLIndex();
    role =
        RoleEntity.builder()
            .withId(-1L)
            .withName("tmp")
            .withSecurableObjects(Lists.newArrayList(tableObject))
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    plugin.onRoleUpdated(role, roleChange);
  }

  @Test
  public void testOwnerManagement() {

    // Test metalake object
    Owner owner = new TemporaryOwner("tmp", Owner.Type.USER);
    MetadataObject metalakeObject =
        MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE);
    expectSQLs = Lists.newArrayList("CREATE USER tmp");
    currentSQLIndex = 0;
    expectTypes.add(MetadataObject.Type.SCHEMA);
    expectObjectNames.add("*");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);

    expectTypes.add(MetadataObject.Type.TABLE);
    expectObjectNames.add("*.*");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);
    plugin.onOwnerSet(metalakeObject, null, owner);

    // clean up
    cleanup();
    expectSQLs = Lists.newArrayList("CREATE USER tmp");

    // Test catalog object
    MetadataObject catalogObject = MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);
    expectTypes.add(MetadataObject.Type.SCHEMA);
    expectObjectNames.add("*");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);

    expectTypes.add(MetadataObject.Type.TABLE);
    expectObjectNames.add("*.*");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);
    plugin.onOwnerSet(catalogObject, null, owner);

    // clean up
    cleanup();
    expectSQLs = Lists.newArrayList("CREATE USER tmp");

    // Test schema object
    MetadataObject schemaObject =
        MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA);
    expectTypes.add(MetadataObject.Type.SCHEMA);
    expectObjectNames.add("schema");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);

    expectTypes.add(MetadataObject.Type.TABLE);
    expectObjectNames.add("schema.*");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);
    plugin.onOwnerSet(schemaObject, null, owner);

    // clean up
    cleanup();
    expectSQLs = Lists.newArrayList("CREATE USER tmp");

    // Test table object
    MetadataObject tableObject =
        MetadataObjects.of(
            Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.TABLE);

    expectTypes.add(MetadataObject.Type.TABLE);
    expectObjectNames.add("schema.table");
    expectPreOwners.add(Optional.empty());
    expectNewOwners.add(owner);
    plugin.onOwnerSet(tableObject, null, owner);
  }

  private static void resetSQLIndex() {
    currentSQLIndex = 0;
  }

  private static void cleanup() {
    expectTypes.clear();
    expectObjectNames.clear();
    expectPreOwners.clear();
    expectNewOwners.clear();
    currentIndex = 0;
    currentSQLIndex = 0;
  }

  private static class TemporaryOwner implements Owner {
    private final String name;
    private final Type type;

    public TemporaryOwner(String name, Type type) {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }
  }

  private static Role createRole(String name) {
    return RoleEntity.builder().withId(0L).withName(name).withAuditInfo(AuditInfo.EMPTY).build();
  }

  private static Group createGroup(String name) {
    return GroupEntity.builder().withId(0L).withName(name).withAuditInfo(AuditInfo.EMPTY).build();
  }

  private static User createUser(String name) {
    return UserEntity.builder().withId(0L).withName(name).withAuditInfo(AuditInfo.EMPTY).build();
  }
}
