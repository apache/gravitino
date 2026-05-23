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
package org.apache.gravitino.idp.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import org.apache.gravitino.Configs;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.mapper.AbstractIdpMetaStorageTest;
import org.apache.gravitino.idp.storage.relational.IdpRelationalEntityStore;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIdpUserGroupManager extends AbstractIdpMetaStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testUserAndGroupLifecycle(String backendType) throws IOException {
    init(backendType);

    getConfig().set(Configs.CACHE_ENABLED, false);
    IdpRelationalEntityStore entityStore = new IdpRelationalEntityStore();
    entityStore.initialize(getConfig());
    IdpUserGroupManager manager = new IdpUserGroupManager(entityStore, RandomIdGenerator.INSTANCE);

    IdpUser user = manager.addUser("alice", "password123");
    assertEquals("alice", user.name());

    assertThrows(AlreadyExistsException.class, () -> manager.addUser("alice", "password456"));
    assertEquals("alice", manager.getUser("alice").name());

    IdpGroup group = manager.addGroup("dev");
    assertEquals("dev", group.name());
    manager.addUsersToGroup("dev", Lists.newArrayList("alice"));
    assertTrue(manager.getGroup("dev").userNames().contains("alice"));

    manager.resetPassword("alice", "new-password");
    assertEquals("alice", manager.getUser("alice").name());
    assertThrows(NotFoundException.class, () -> manager.resetPassword("missing", "pwd"));

    manager.removeUsersFromGroup("dev", Lists.newArrayList("alice"));
    assertTrue(manager.removeGroup("dev", false));

    assertTrue(manager.removeUser("alice"));
    assertFalse(manager.removeUser("missing"));
    assertThrows(NotFoundException.class, () -> manager.getUser("alice"));

    entityStore.close();
  }
}
