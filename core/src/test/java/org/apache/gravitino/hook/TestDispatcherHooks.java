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
package org.apache.gravitino.hook;

import static org.apache.gravitino.Configs.SERVICE_ADMINS;

import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDispatcherHooks {

  @Test
  public void testLifecycleHooks() throws IllegalAccessException {
    Config config = new Config(false) {};
    config.set(SERVICE_ADMINS, Lists.newArrayList("admin1", "admin2"));
    EntityStore entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);
    IdGenerator idGenerator = new RandomIdGenerator();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);

    DispatcherHooks hooks = new DispatcherHooks();
    AtomicBoolean result = new AtomicBoolean(true);
    hooks.addPostHook(
        "createMetalake",
        (args, metalake) -> {
          result.set(false);
        });
    MetalakeDispatcher metalakeDispatcher =
        DispatcherHookHelper.installHooks(new MetalakeManager(entityStore, idGenerator), hooks);
    Assertions.assertTrue(result.get());
    metalakeDispatcher.createMetalake(NameIdentifier.of("test"), "", Collections.emptyMap());
    Assertions.assertFalse(result.get());

    hooks.addPostHook(
        "addUser",
        (args, user) -> {
          result.set(false);
        });
    AccessControlDispatcher accessControlManager =
        DispatcherHookHelper.installHooks(
            new AccessControlManager(entityStore, idGenerator, config), hooks);
    result.set(true);
    Assertions.assertTrue(result.get());
    accessControlManager.addUser("test", "test");
    Assertions.assertFalse(result.get());
  }
}
