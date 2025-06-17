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
package org.apache.gravitino.authorization;

import static org.apache.gravitino.Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.SERVICE_ADMINS;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestAccessControlManager {

  private static AccessControlManager accessControlManager;

  private static EntityStore entityStore;
  private static CatalogManager catalogManager = mock(CatalogManager.class);

  private static final Config config = Mockito.mock(Config.class);

  private static String METALAKE = "metalake";
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";

  private static AuthorizationPlugin authorizationPlugin;

  private static BaseMetalake metalakeEntity =
      BaseMetalake.builder()
          .withId(1L)
          .withName(METALAKE)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  private static BaseMetalake listMetalakeEntity =
      BaseMetalake.builder()
          .withId(2L)
          .withName("metalake_list")
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  @BeforeAll
  public static void setUp() throws Exception {
    File dbDir = new File(DB_DIR);
    dbDir.mkdirs();

    Mockito.when(config.get(SERVICE_ADMINS)).thenReturn(Lists.newArrayList("admin1", "admin2"));
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    Mockito.when(config.get(CATALOG_CACHE_EVICTION_INTERVAL_MS)).thenReturn(1000L);
    // Fix cache for testing.
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    entityStore.put(metalakeEntity, true);
    entityStore.put(listMetalakeEntity, true);

    CatalogEntity catalogEntity =
        CatalogEntity.builder()
            .withId(3L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(catalogEntity, true);

    CatalogEntity anotherCatalogEntity =
        CatalogEntity.builder()
            .withId(4L)
            .withName("catalog")
            .withNamespace(Namespace.of("metalake_list"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    entityStore.put(anotherCatalogEntity, true);

    accessControlManager = new AccessControlManager(entityStore, new RandomIdGenerator(), config);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", accessControlManager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    BaseCatalog catalog = mock(BaseCatalog.class);
    when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    authorizationPlugin = mock(AuthorizationPlugin.class);
    when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testAddUser() {
    User user = accessControlManager.addUser(METALAKE, "testAdd");
    Assertions.assertEquals("testAdd", user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    user = accessControlManager.addUser(METALAKE, "testAddWithOptionalField");

    Assertions.assertEquals("testAddWithOptionalField", user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.addUser("no-exist", "testAdd"));

    // Test with UserAlreadyExistsException
    Assertions.assertThrows(
        UserAlreadyExistsException.class, () -> accessControlManager.addUser(METALAKE, "testAdd"));
  }

  @Test
  public void testGetUser() {
    accessControlManager.addUser(METALAKE, "testGet");

    User user = accessControlManager.getUser(METALAKE, "testGet");
    Assertions.assertEquals("testGet", user.name());

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.getUser("no-exist", "testAdd"));

    // Test to get non-existed user
    Throwable exception =
        Assertions.assertThrows(
            NoSuchUserException.class, () -> accessControlManager.getUser(METALAKE, "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("User not-exist does not exist"));
  }

  @Test
  public void testRemoveUser() {
    accessControlManager.addUser(METALAKE, "testRemove");

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.removeUser("no-exist", "testAdd"));

    // Test to remove user
    boolean removed = accessControlManager.removeUser(METALAKE, "testRemove");
    Assertions.assertTrue(removed);

    // Test to remove non-existed user
    boolean removed1 = accessControlManager.removeUser(METALAKE, "no-exist");
    Assertions.assertFalse(removed1);
  }

  @Test
  public void testListUsers() {
    accessControlManager.addUser("metalake_list", "testList1");
    accessControlManager.addUser("metalake_list", "testList2");

    // Test to list users
    String[] expectUsernames = new String[] {"testList1", "testList2"};
    String[] actualUsernames = accessControlManager.listUserNames("metalake_list");
    Arrays.sort(actualUsernames);
    Assertions.assertArrayEquals(expectUsernames, actualUsernames);
    User[] users = accessControlManager.listUsers("metalake_list");
    Arrays.sort(users, Comparator.comparing(User::name));
    Assertions.assertArrayEquals(
        expectUsernames, Arrays.stream(users).map(User::name).toArray(String[]::new));

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.listUserNames("no-exist"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.listUsers("no-exist"));
  }

  @Test
  public void testAddGroup() {
    Group group = accessControlManager.addGroup(METALAKE, "testAdd");
    Assertions.assertEquals("testAdd", group.name());
    Assertions.assertTrue(group.roles().isEmpty());

    group = accessControlManager.addGroup(METALAKE, "testAddWithOptionalField");

    Assertions.assertEquals("testAddWithOptionalField", group.name());
    Assertions.assertTrue(group.roles().isEmpty());

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.addGroup("no-exist", "testAdd"));

    // Test with GroupAlreadyExistsException
    Assertions.assertThrows(
        GroupAlreadyExistsException.class,
        () -> accessControlManager.addGroup(METALAKE, "testAdd"));
  }

  @Test
  public void testGetGroup() {
    accessControlManager.addGroup(METALAKE, "testGet");

    Group group = accessControlManager.getGroup(METALAKE, "testGet");
    Assertions.assertEquals("testGet", group.name());

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.getGroup("no-exist", "testAdd"));

    // Test to get non-existed group
    Throwable exception =
        Assertions.assertThrows(
            NoSuchGroupException.class, () -> accessControlManager.getGroup(METALAKE, "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("Group not-exist does not exist"));
  }

  @Test
  public void testListGroupss() {
    accessControlManager.addGroup("metalake_list", "testList1");
    accessControlManager.addGroup("metalake_list", "testList2");

    // Test to list groups
    String[] expectGroupNames = new String[] {"testList1", "testList2"};
    String[] actualGroupNames = accessControlManager.listGroupNames("metalake_list");
    Arrays.sort(actualGroupNames);
    Assertions.assertArrayEquals(expectGroupNames, actualGroupNames);
    Group[] groups = accessControlManager.listGroups("metalake_list");
    Arrays.sort(groups, Comparator.comparing(Group::name));
    Assertions.assertArrayEquals(
        expectGroupNames, Arrays.stream(groups).map(Group::name).toArray(String[]::new));

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.listGroupNames("no-exist"));
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> accessControlManager.listGroups("no-exist"));
  }

  @Test
  public void testRemoveGroup() {
    accessControlManager.addGroup(METALAKE, "testRemove");

    // Test with NoSuchMetalakeException
    Assertions.assertThrows(
        NoSuchMetalakeException.class,
        () -> accessControlManager.removeGroup("no-exist", "testAdd"));

    // Test to remove group
    boolean removed = accessControlManager.removeGroup(METALAKE, "testRemove");
    Assertions.assertTrue(removed);

    // Test to remove non-existed group
    boolean removed1 = accessControlManager.removeGroup(METALAKE, "no-exist");
    Assertions.assertFalse(removed1);
  }

  @Test
  public void testServiceAdmin() {
    Assertions.assertTrue(accessControlManager.isServiceAdmin("admin1"));
    Assertions.assertTrue(accessControlManager.isServiceAdmin("admin2"));
    Assertions.assertFalse(accessControlManager.isServiceAdmin("admin3"));
  }

  @Test
  public void testCreateRole() {
    reset(authorizationPlugin);
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    Role role =
        accessControlManager.createRole(
            METALAKE,
            "create",
            props,
            Lists.newArrayList(
                SecurableObjects.ofCatalog(
                    "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));
    Assertions.assertEquals("create", role.name());
    testProperties(props, role.properties());
    verify(authorizationPlugin).onRoleCreated(any());

    // Test with RoleAlreadyExistsException
    Assertions.assertThrows(
        RoleAlreadyExistsException.class,
        () ->
            accessControlManager.createRole(
                METALAKE,
                "create",
                props,
                Lists.newArrayList(
                    SecurableObjects.ofCatalog(
                        "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())))));
  }

  @Test
  public void testLoadRole() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createRole(
        METALAKE,
        "loadRole",
        props,
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    Role role = accessControlManager.getRole(METALAKE, "loadRole");

    Assertions.assertEquals("loadRole", role.name());
    testProperties(props, role.properties());

    // Test load non-existed role
    Throwable exception =
        Assertions.assertThrows(
            NoSuchRoleException.class, () -> accessControlManager.getRole(METALAKE, "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("Role not-exist does not exist"));
  }

  @Test
  public void testDropRole() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createRole(
        METALAKE,
        "testDrop",
        props,
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // Test drop role
    reset(authorizationPlugin);
    boolean dropped = accessControlManager.deleteRole(METALAKE, "testDrop");
    Assertions.assertTrue(dropped);

    verify(authorizationPlugin).onRoleDeleted(any());

    // Test drop non-existed role
    boolean dropped1 = accessControlManager.deleteRole(METALAKE, "no-exist");
    Assertions.assertFalse(dropped1);
  }

  @Test
  public void testListRoles() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createRole(
        "metalake_list",
        "testList1",
        props,
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    accessControlManager.createRole(
        "metalake_list",
        "testList2",
        props,
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))));

    // Test to list roles
    String[] actualRoles = accessControlManager.listRoleNames("metalake_list");
    Arrays.sort(actualRoles);
    Assertions.assertArrayEquals(new String[] {"testList1", "testList2"}, actualRoles);

    accessControlManager.deleteRole("metalake_list", "testList1");
    accessControlManager.deleteRole("metalake_list", "testList2");
  }

  @Test
  public void testListRolesByObject() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    accessControlManager.createRole(
        "metalake_list", "testList1", props, Lists.newArrayList(catalogObject));

    accessControlManager.createRole(
        "metalake_list", "testList2", props, Lists.newArrayList(catalogObject));

    // Test to list roles
    String[] listedRoles =
        accessControlManager.listRoleNamesByObject("metalake_list", catalogObject);
    Arrays.sort(listedRoles);
    Assertions.assertArrayEquals(new String[] {"testList1", "testList2"}, listedRoles);

    accessControlManager.deleteRole("metalake_list", "testList1");
    accessControlManager.deleteRole("metalake_list", "testList2");
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertFalse(testProps.containsKey(StringIdentifier.ID_KEY));
  }
}
