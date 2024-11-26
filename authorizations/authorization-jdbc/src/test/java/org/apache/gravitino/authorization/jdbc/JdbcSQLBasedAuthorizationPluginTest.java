package org.apache.gravitino.authorization.jdbc;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.gravitino.authorization.jdbc.JdbcSQLBasedAuthorizationPlugin.JDBC_DRIVER;
import static org.apache.gravitino.authorization.jdbc.JdbcSQLBasedAuthorizationPlugin.JDBC_PASSWORD;
import static org.apache.gravitino.authorization.jdbc.JdbcSQLBasedAuthorizationPlugin.JDBC_URL;
import static org.apache.gravitino.authorization.jdbc.JdbcSQLBasedAuthorizationPlugin.JDBC_USERNAME;
import static org.apache.gravitino.integration.test.container.HiveContainer.ENABLE_AUTHORIZATION;
import static org.apache.gravitino.integration.test.container.HiveContainer.HIVE3;
import static org.apache.gravitino.integration.test.container.HiveContainer.HIVE_RUNTIME_VERSION;

@Tag("gravitino-docker-test")
class JdbcSQLBasedAuthorizationPluginTest {

    private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
    private static JdbcSQLBasedAuthorizationPlugin plugin;
    private static final AuditInfo auditInfo = AuditInfo.EMPTY;
    private static final String SHOW_ROLES = "SHOW ROLES";
    private static final String SHOW_USER_GRANT_ROLES = "SHOW ROLE GRANT USER %s";
    private static final String SHOW_PRIVILEGES_ROLE = "SHOW GRANT ROLE %s ON ALL";

    private static final String SHOW_TABLE_OWNER = "SHOW TABLE EXTENDED LIKE '%s'";
    private static final String SHOW_DATABASE_OWNER = "DESCRIBE DATABASE %s";
    @BeforeAll
    public static void beforeAll() {
        containerSuite.startJdbcHiveContainer(ImmutableMap.of(ENABLE_AUTHORIZATION, "true", HIVE_RUNTIME_VERSION, HIVE3));
        Map<String, String> properties = Maps.newHashMap();
        properties.put(JDBC_DRIVER, "org.apache.hive.jdbc.HiveDriver");
        properties.put(JDBC_URL, String.format("jdbc:hive2://%s:10000/default", containerSuite.getJdbcHiveContainer().getContainerIpAddress()));
        properties.put(JDBC_USERNAME, "hive");
        properties.put(JDBC_PASSWORD, "");
        plugin = new HiveJdbcSQLBasedAuthorizationPlugin(properties);
    }

    @AfterAll
    public static void afterAll()  throws IOException {
        plugin.close();
    }


    @Test
    public void testRole() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("role").withAuditInfo(auditInfo).build();

        plugin.beforeExecuteSQL();

        assertQuery(SHOW_ROLES, resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertFalse(roles.contains(roleEntity.name()));
        });

        // case1: create a role
        plugin.onRoleCreated(roleEntity);

        assertQuery(SHOW_ROLES, resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertTrue(roles.contains(roleEntity.name()));
        });

        // case2: create a duplicated role
        plugin.onRoleCreated(roleEntity);

        // case 3: acquire a role
        Assertions.assertThrows(UnsupportedOperationException.class, () -> plugin.onRoleAcquired(roleEntity));

        // case 4: delete a role
        plugin.onRoleDeleted(roleEntity);

        assertQuery(SHOW_ROLES, resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertFalse(roles.contains(roleEntity.name()));
        });
    }

    @Test
    public void testTablePrivilegeManagement() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("role").withAuditInfo(auditInfo).build();

        SecurableObject catalog = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schema = SecurableObjects.ofSchema(catalog, "default", Lists.newArrayList());
        SecurableObject table = SecurableObjects.ofTable(schema, "employee", Lists.newArrayList(Privileges.SelectTable.allow()));
        SecurableObject newTable = SecurableObjects.ofTable(schema, "employee", Lists.newArrayList(Privileges.ModifyTable.allow()));

        plugin.beforeExecuteSQL();

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.removeSecurableObject(roleEntity.name(), table));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.addSecurableObject(roleEntity.name(), table));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), table.name());
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.SELECT.name()));
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.updateSecurableObject(roleEntity.name(), table, newTable));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), table.name());
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.UPDATE.name()));
        });
    }

    @Test
    public void testDatabasePrivilegeManagement() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("role2").withAuditInfo(auditInfo).build();
        SecurableObject catalog = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schema = SecurableObjects.ofSchema(catalog, "default", Lists.newArrayList(Privileges.SelectTable.allow()));
        SecurableObject newSchema = SecurableObjects.ofSchema(catalog, "default", Lists.newArrayList(Privileges.ModifyTable.allow()));

        plugin.beforeExecuteSQL();

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.removeSecurableObject(roleEntity.name(), schema));
        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.addSecurableObject(roleEntity.name(), schema));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.SELECT.name()));
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.updateSecurableObject(roleEntity.name(), schema, newSchema));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.UPDATE.name()));
        });
    }

    @Test
    public void testCatalogPrivilegeManagement() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("role3").withAuditInfo(auditInfo).build();
        SecurableObject catalog = SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.SelectTable.allow()));
        SecurableObject newCatalog = SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.ModifyTable.allow()));

        plugin.beforeExecuteSQL();
        plugin.executeUpdateSQL("CREATE DATABASE TEST", "EXISTS");

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.removeSecurableObject(roleEntity.name(), catalog));
        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.addSecurableObject(roleEntity.name(), catalog));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of("default", "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.SELECT.name()));
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.updateSecurableObject(roleEntity.name(), catalog, newCatalog));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of("default", "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.UPDATE.name()));
        });
    }

    @Test
    public void testMetalakePrivilegeManagement() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("role4").withAuditInfo(auditInfo).build();
        SecurableObject catalog = SecurableObjects.ofMetalake("metalake", Lists.newArrayList());
        SecurableObject schema = SecurableObjects.ofSchema(catalog, "default", Lists.newArrayList(Privileges.SelectTable.allow()));
        SecurableObject newSchema = SecurableObjects.ofSchema(catalog, "default", Lists.newArrayList(Privileges.ModifyTable.allow()));

        plugin.beforeExecuteSQL();

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.removeSecurableObject(roleEntity.name(), schema));
        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Assertions.assertTrue(privileges.isEmpty());
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.addSecurableObject(roleEntity.name(), schema));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.SELECT.name()));
        });

        plugin.onRoleUpdated(roleEntity, RoleChange.updateSecurableObject(roleEntity.name(), schema, newSchema));

        assertQuery(String.format(SHOW_PRIVILEGES_ROLE, roleEntity.name()), resultSet -> {
            Map<Pair<String, String>, List<String>> privileges = getPrivileges(resultSet);
            Pair<String, String> pair = Pair.of(schema.name(), "");
            Assertions.assertTrue(privileges.get(pair).contains(JdbcPrivilege.Type.UPDATE.name()));
        });
    }


    @Test
    public void testPermissionManagement() throws SQLException {
        RoleEntity roleEntity = RoleEntity.builder().withId(1L).withName("manager").withAuditInfo(auditInfo).build();
        UserEntity userEntity = UserEntity.builder().withId(1L).withName("hive").withAuditInfo(auditInfo).build();

        plugin.beforeExecuteSQL();
        assertQuery(String.format(SHOW_USER_GRANT_ROLES, userEntity.name()), resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertFalse(roles.contains(roleEntity.name()));
        });
        plugin.onGrantedRolesToUser(Lists.newArrayList(roleEntity), userEntity);

        assertQuery(String.format(SHOW_USER_GRANT_ROLES, userEntity.name()), resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertTrue(roles.contains(roleEntity.name()));
        });

        plugin.onRevokedRolesFromUser(Lists.newArrayList(roleEntity), userEntity);

        assertQuery(String.format(SHOW_USER_GRANT_ROLES, userEntity.name()), resultSet -> {
            Set<String> roles = getRoles(resultSet);
            Assertions.assertFalse(roles.contains(roleEntity.name()));
        });
    }


    @Test
    public void testOwnerManagement() throws SQLException {
        MetadataObject tableObject =  MetadataObjects.of(Lists.newArrayList("catalog", "default", "employee"), MetadataObject.Type.TABLE);
        Owner legacyOwner = new TemporaryOwner("root", Owner.Type.USER);
        Owner newOwner = new TemporaryOwner("new", Owner.Type.USER);

         assertQuery(String.format(SHOW_TABLE_OWNER, "employee"), resultSet -> {
            String owner = getTableOwner(resultSet);
            Assertions.assertEquals(owner, legacyOwner.name());
         });
        plugin.onOwnerSet(tableObject, legacyOwner, newOwner);
        assertQuery(String.format(SHOW_TABLE_OWNER, "employee"), resultSet -> {
            String owner = getTableOwner(resultSet);
            Assertions.assertEquals(owner, newOwner.name());
        });

        MetadataObject schemaObject =  MetadataObjects.of(Lists.newArrayList("catalog", "default"), MetadataObject.Type.SCHEMA);
        Owner databaseLegacyOwner = new TemporaryOwner("public", Owner.Type.USER);
        assertQuery(String.format(SHOW_DATABASE_OWNER, "default"), resultSet -> {
            String owner = getDatabaseOwner(resultSet);
            Assertions.assertEquals(owner, databaseLegacyOwner.name());
        });
        plugin.onOwnerSet(schemaObject, legacyOwner, newOwner);
        assertQuery(String.format(SHOW_DATABASE_OWNER, "default"), resultSet -> {
            String owner = getDatabaseOwner(resultSet);
            Assertions.assertEquals(owner, newOwner.name());
        });
    }

    private static class TemporaryOwner implements Owner {
        private final String name;
        private final Type type;
        TemporaryOwner(String name, Type type) {
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


    private static void assertQuery(String sql, Consumer<ResultSet> asserter) throws SQLException {
        try (final Connection connection = plugin.getConnection()) {
            try (final Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    asserter.accept(resultSet);
                }
            }
        }
    }

    private static String getDatabaseOwner(ResultSet resultSet) {
        try {
            while (resultSet.next()) {
                return resultSet.getString(4);
            }
            throw new RuntimeException("Every database should have owner");
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static String getTableOwner(ResultSet resultSet) {
        try {
            while (resultSet.next()) {
                String info = resultSet.getString(1);
                String[] splits = info.split(":");
                if (splits[0].equals("owner")) {
                    return splits[1];
                }
            }
            throw new RuntimeException("Every table should have owner");
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static Map<Pair<String, String>, List<String>> getPrivileges(ResultSet resultSet) {
        try {
            Map<Pair<String, String>, List<String>> privileges = Maps.newHashMap();
            while (resultSet.next()) {
               Pair<String, String> resource =  Pair.of(resultSet.getString(1) , resultSet.getString(2));
               String privilege = resultSet.getString(7);
               privileges.compute(resource, (k, v) -> {
                   if (v == null) {
                       v = Lists.newArrayList();
                   }
                   v.add(privilege);
                   return v;
               });
            }
            return privileges;
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static Set<String> getRoles(ResultSet resultSet) {
        try {
            Set<String> roles = Sets.newHashSet();
            while (resultSet.next()) {
                int count = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= count; i++) {
                    roles.add(resultSet.getString(i));
                }

            }
            return roles;
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }
}