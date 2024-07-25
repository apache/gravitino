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

import static org.apache.gravitino.Entity.SYSTEM_METALAKE_RESERVED_NAME;

import java.io.IOException;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestMetalakeNormalizeDispatcher {
  private static MetalakeNormalizeDispatcher metalakeNormalizeDispatcher;
  private static EntityStore entityStore;

  @BeforeAll
  public static void setUp() {
    Config config = new Config(false) {};

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    MetalakeManager metalakeManager = new MetalakeManager(entityStore, new RandomIdGenerator());
    metalakeNormalizeDispatcher = new MetalakeNormalizeDispatcher(metalakeManager);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testNameSpec() {
    // Test for valid names
    String[] legalNames = {"metalake", "_metalake", "1_metalake", "_", "1"};
    for (String legalName : legalNames) {
      NameIdentifier metalakeIdent = NameIdentifier.of(legalName);
      Metalake metalake = metalakeNormalizeDispatcher.createMetalake(metalakeIdent, null, null);
      Assertions.assertEquals(legalName, metalake.name());
    }

    // Test for illegal and reserved names
    NameIdentifier metalakeIdent1 = NameIdentifier.of(SYSTEM_METALAKE_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent1, null, null));
    Assertions.assertEquals("The metalake name 'system' is reserved.", exception.getMessage());

    String[] illegalNames = {
      "metalake-xxx",
      "metalake/xxx",
      "metalake.xxx",
      "metalake@xxx",
      "metalake#xxx",
      "metalake$xxx",
      "metalake%xxx",
      "metalake^xxx",
      "metalake&xxx",
      "metalake*xxx",
      "metalake+xxx",
      "metalake=xxx",
      "metalake|xxx",
      "metalake\\xxx",
      "metalake`xxx",
      "metalake~xxx",
      "metalake!xxx",
      "metalake\"xxx",
      "metalake'xxx",
      "metalake<xxx",
      "metalake>xxx",
      "metalake,xxx",
      "metalake?xxx",
      "metalake:xxx",
      "metalake;xxx",
      "metalake[xxx",
      "metalake]xxx",
      "metalake{xxx",
      "metalake}xxx"
    };
    for (String illegalName : illegalNames) {
      NameIdentifier metalakeIdent = NameIdentifier.of(illegalName);
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent, null, null));
      Assertions.assertEquals(
          "The metalake name '" + illegalName + "' is illegal.", exception.getMessage());
    }
  }
}
