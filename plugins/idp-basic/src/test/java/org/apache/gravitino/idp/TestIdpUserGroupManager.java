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
package org.apache.gravitino.idp;

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link IdpUserGroupManager} backed by an embedded H2 store. */
public class TestIdpUserGroupManager {

  private static IdpUserGroupManager manager;
  private static Path h2Path;

  @BeforeAll
  public static void setUp() throws Exception {
    h2Path = Files.createTempDirectory("gravitino_idp_manager_h2_");

    Config config = new Config(false) {};
    config.set(ENTITY_STORE, RELATIONAL_ENTITY_STORE);
    config.set(ENTITY_RELATIONAL_STORE, "h2");
    config.set(
        ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 100);
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);
    config.set(STORE_DELETE_AFTER_TIME, 20 * 60 * 1000L);
    config.set(CACHE_ENABLED, false);

    manager = new IdpUserGroupManager(config, RandomIdGenerator.INSTANCE);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (manager != null) {
      manager.close();
      manager = null;
    }

    if (h2Path != null && Files.exists(h2Path)) {
      try (Stream<Path> paths = Files.walk(h2Path)) {
        paths.sorted(Comparator.reverseOrder()).forEach(TestIdpUserGroupManager::deletePath);
      }
    }
  }

  @Test
  public void testAddUser() throws IOException {
    IdpUser user = manager.addUser("testAdd", "password123");
    Assertions.assertEquals("testAdd", user.name());
    Assertions.assertTrue(user.groupNames().isEmpty());

    Assertions.assertThrows(
        AlreadyExistsException.class, () -> manager.addUser("testAdd", "password456"));
  }

  @Test
  public void testGetUser() throws IOException {
    manager.addUser("testGet", "password123");

    IdpUser user = manager.getUser("testGet");
    Assertions.assertEquals("testGet", user.name());

    Throwable exception =
        Assertions.assertThrows(NotFoundException.class, () -> manager.getUser("not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("IdP user not found: not-exist"));
  }

  @Test
  public void testRemoveUser() throws IOException {
    manager.addUser("testRemove", "password123");

    Assertions.assertTrue(manager.removeUser("testRemove"));
    Assertions.assertFalse(manager.removeUser("no-exist"));
  }

  @Test
  public void testChangePassword() throws IOException {
    manager.addUser("testChangePassword", "password123");

    Assertions.assertTrue(manager.changePassword("testChangePassword", "new-password"));
    Assertions.assertEquals("testChangePassword", manager.getUser("testChangePassword").name());

    Assertions.assertFalse(manager.changePassword("not-exist", "password123"));
  }

  @Test
  public void testAddGroup() throws IOException {
    IdpGroup group = manager.addGroup("testAddGroup");
    Assertions.assertEquals("testAddGroup", group.name());
    Assertions.assertTrue(group.usernames().isEmpty());

    Assertions.assertThrows(AlreadyExistsException.class, () -> manager.addGroup("testAddGroup"));
  }

  @Test
  public void testGetGroup() throws IOException {
    manager.addGroup("testGetGroup");

    IdpGroup group = manager.getGroup("testGetGroup");
    Assertions.assertEquals("testGetGroup", group.name());

    Throwable exception =
        Assertions.assertThrows(NotFoundException.class, () -> manager.getGroup("not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("IdP group not found: not-exist"));
  }

  @Test
  public void testChangeGroupMembership() throws IOException {
    manager.addUser("groupUser1", "password123");
    manager.addUser("groupUser2", "password123");
    manager.addUser("groupUser3", "password123");
    manager.addGroup("testMembershipGroup");

    IdpGroup group =
        manager.changeGroupMembership(
            "testMembershipGroup", Lists.newArrayList("groupUser1", "groupUser2"), null);
    Assertions.assertTrue(group.usernames().contains("groupUser1"));
    Assertions.assertTrue(group.usernames().contains("groupUser2"));

    group =
        manager.changeGroupMembership(
            "testMembershipGroup",
            Lists.newArrayList("groupUser3"),
            Lists.newArrayList("groupUser1"));
    Assertions.assertFalse(group.usernames().contains("groupUser1"));
    Assertions.assertTrue(group.usernames().contains("groupUser2"));
    Assertions.assertTrue(group.usernames().contains("groupUser3"));

    group =
        manager.changeGroupMembership(
            "testMembershipGroup", null, Lists.newArrayList("groupUser2", "groupUser3"));
    Assertions.assertTrue(group.usernames().isEmpty());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> manager.changeGroupMembership("testMembershipGroup", null, null));
  }

  @Test
  public void testRemoveGroup() throws IOException {
    manager.addUser("groupMember", "password123");
    manager.addGroup("testRemoveGroup");
    manager.changeGroupMembership("testRemoveGroup", Lists.newArrayList("groupMember"), null);

    Assertions.assertThrows(
        IllegalStateException.class, () -> manager.removeGroup("testRemoveGroup", false));

    manager.changeGroupMembership("testRemoveGroup", null, Lists.newArrayList("groupMember"));
    Assertions.assertTrue(manager.removeGroup("testRemoveGroup", false));
    Assertions.assertFalse(manager.removeGroup("no-exist", false));

    manager.addUser("forceMember", "password123");
    manager.addGroup("testForceRemoveGroup");
    manager.changeGroupMembership("testForceRemoveGroup", Lists.newArrayList("forceMember"), null);
    Assertions.assertTrue(manager.removeGroup("testForceRemoveGroup", true));
  }

  private static void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Delete path failed: " + path, e);
    }
  }
}
