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
package org.apache.gravitino.metalake;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.Mockito.doReturn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.shaded.org.apache.commons.lang3.reflect.FieldUtils;

public class TestMetalakeManager {

  private static MetalakeManager metalakeManager;

  private static EntityStore entityStore;

  private static Config config;

  @BeforeAll
  public static void setUp() throws IllegalAccessException {
    config = Mockito.mock(Config.class);

    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    metalakeManager = new MetalakeManager(entityStore, new RandomIdGenerator());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testCreateMetalake() {
    NameIdentifier ident = NameIdentifier.of("test1");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test1", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    testProperties(props, metalake.properties());

    // Test with MetalakeAlreadyExistsException
    Assertions.assertThrows(
        MetalakeAlreadyExistsException.class,
        () -> metalakeManager.createMetalake(ident, "comment", props));
  }

  @Test
  public void testListMetalakes() {
    NameIdentifier ident1 = NameIdentifier.of("test11");
    NameIdentifier ident2 = NameIdentifier.of("test12");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    metalakeManager.createMetalake(ident1, "comment", props);
    BaseMetalake metalake1 = metalakeManager.loadMetalake(ident1);
    metalakeManager.createMetalake(ident2, "comment", props);
    BaseMetalake metalake2 = metalakeManager.loadMetalake(ident2);

    Set<BaseMetalake> metalakes = Sets.newHashSet(metalakeManager.listMetalakes());
    Assertions.assertTrue(metalakes.contains(metalake1));
    Assertions.assertTrue(metalakes.contains(metalake2));
  }

  @Test
  public void testLoadMetalake() {
    NameIdentifier ident = NameIdentifier.of("test21");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test21", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    testProperties(props, metalake.properties());

    BaseMetalake loadedMetalake = metalakeManager.loadMetalake(ident);
    Assertions.assertEquals("test21", loadedMetalake.name());
    Assertions.assertEquals("comment", loadedMetalake.comment());
    testProperties(props, loadedMetalake.properties());

    // Test with NoSuchMetalakeException
    NameIdentifier id = NameIdentifier.of("test3");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> metalakeManager.loadMetalake(id));
    Assertions.assertTrue(exception.getMessage().contains("Metalake test3 does not exist"));
  }

  @Test
  public void testAlterMetalake() throws Exception {
    NameIdentifier ident = NameIdentifier.of("test31");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test31", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    testProperties(props, metalake.properties());

    // Test alter name;
    MetalakeChange change = MetalakeChange.rename("test32");
    BaseMetalake alteredMetalake = metalakeManager.alterMetalake(ident, change);
    Assertions.assertEquals("test32", alteredMetalake.name());
    Assertions.assertEquals("comment", alteredMetalake.comment());
    testProperties(props, alteredMetalake.properties());

    // Test alter comment;
    NameIdentifier ident1 = NameIdentifier.of("test32");
    MetalakeChange change1 = MetalakeChange.updateComment("comment2");
    BaseMetalake alteredMetalake1 = metalakeManager.alterMetalake(ident1, change1);
    Assertions.assertEquals("test32", alteredMetalake1.name());
    Assertions.assertEquals("comment2", alteredMetalake1.comment());
    testProperties(props, alteredMetalake1.properties());

    // test alter properties;
    MetalakeChange change2 = MetalakeChange.setProperty("key2", "value2");
    MetalakeChange change3 = MetalakeChange.setProperty("key3", "value3");
    MetalakeChange change4 = MetalakeChange.removeProperty("key3");

    BaseMetalake alteredMetalake2 =
        metalakeManager.alterMetalake(ident1, change2, change3, change4);
    Assertions.assertEquals("test32", alteredMetalake2.name());
    Assertions.assertEquals("comment2", alteredMetalake2.comment());
    Map<String, String> expectedProps = ImmutableMap.of("key1", "value1", "key2", "value2");
    testProperties(expectedProps, alteredMetalake2.properties());

    // Test with NoSuchMetalakeException
    NameIdentifier id = NameIdentifier.of("test3");
    Throwable exception =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> metalakeManager.alterMetalake(id, change));
    Assertions.assertTrue(exception.getMessage().contains("Metalake test3 does not exist"));

    // Test the audit info
    UserPrincipal userPrincipal = new UserPrincipal("test");
    MetalakeChange change5 = MetalakeChange.setProperty("key5", "value5");
    alteredMetalake =
        PrincipalUtils.doAs(userPrincipal, () -> metalakeManager.alterMetalake(ident1, change5));
    Assertions.assertEquals(userPrincipal.getName(), alteredMetalake.auditInfo().lastModifier());
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, alteredMetalake.auditInfo().creator());
  }

  @Test
  public void testDropMetalake() {
    NameIdentifier ident = NameIdentifier.of("test41");
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    BaseMetalake metalake = metalakeManager.createMetalake(ident, "comment", props);
    Assertions.assertEquals("test41", metalake.name());
    Assertions.assertEquals("comment", metalake.comment());
    testProperties(props, metalake.properties());

    metalakeManager.disableMetalake(ident);
    boolean dropped = metalakeManager.dropMetalake(ident);
    Assertions.assertTrue(dropped, "metalake should be dropped");

    // Test with NoSuchMetalakeException
    NameIdentifier ident1 = NameIdentifier.of("test42");
    boolean dropped1 = metalakeManager.dropMetalake(ident1);
    Assertions.assertFalse(dropped1, "metalake should be non-existent");
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertFalse(testProps.containsKey(StringIdentifier.ID_KEY));
  }
}
