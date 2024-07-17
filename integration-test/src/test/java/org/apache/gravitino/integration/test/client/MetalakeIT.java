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
package org.apache.gravitino.integration.test.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeIT extends AbstractIT {
  public static String metalakeNameA = RandomNameUtils.genRandomName("metalakeA");
  public static String metalakeNameB = RandomNameUtils.genRandomName("metalakeB");

  @BeforeEach
  private void start() {
    // Just in case clean up is needed due to a test failure
    dropMetalakes();
  }

  @AfterEach
  private void stop() {
    dropMetalakes();
  }

  @Test
  public void testListMetalake() {
    // no metalakes to start with
    GravitinoMetalake[] metaLakes = client.listMetalakes();
    assertEquals(0, metaLakes.length);

    // one metalakes
    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());
    metaLakes = client.listMetalakes();
    assertEquals(1, metaLakes.length);
    assertEquals(metaLakes[0].name(), metalakeNameA);

    // two metalakes
    client.createMetalake(metalakeNameB, "metalake B comment", Collections.emptyMap());
    metaLakes = client.listMetalakes();
    ArrayList<String> names = new ArrayList<>(2);
    assertEquals(2, metaLakes.length);
    names.add(metaLakes[0].name());
    names.add(metaLakes[1].name());
    assertTrue(names.contains(metalakeNameA));
    assertTrue(names.contains(metalakeNameB));
  }

  @Test
  public void testLoadMetalake() {
    // metalake exists
    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());
    GravitinoMetalake metaLakeA = client.loadMetalake(metalakeNameA);
    assertEquals(metaLakeA.name(), metalakeNameA);

    // metalake does not exist
    NameIdentifier noexist = NameIdentifier.of(metalakeNameB);
    assertThrows(
        NoSuchMetalakeException.class,
        () -> {
          client.loadMetalake(noexist.name());
        });

    // metalake empty name - note it's NameIdentifier.of("") that fails not the load
    assertThrows(IllegalNameIdentifierException.class, () -> client.loadMetalake(""));
  }

  @Test
  public void testAlterMetalake() {
    String newName = RandomNameUtils.genRandomName("newmetaname");

    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());

    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
        };
    GravitinoMetalake metaLake = client.alterMetalake(metalakeNameA, changes);
    assertEquals(newName, metaLake.name());
    assertEquals("new metalake comment", metaLake.comment());
    assertEquals(AuthConstants.ANONYMOUS_USER, metaLake.auditInfo().creator());

    // Reload metadata via new name to check if the changes are applied
    GravitinoMetalake newMetalake = client.loadMetalake(newName);
    assertEquals(newName, newMetalake.name());
    assertEquals("new metalake comment", newMetalake.comment());

    // Old name does not exist
    NameIdentifier old = NameIdentifier.of(metalakeNameA);
    assertThrows(NoSuchMetalakeException.class, () -> client.loadMetalake(old.name()));
  }

  @Test
  public void testAlterNonExistentMetalake() {
    String newName = RandomNameUtils.genRandomName("newmetaname");

    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());

    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
        };

    // rename non existent metalake
    NameIdentifier noexists = NameIdentifier.of(metalakeNameB);
    assertThrows(
        NoSuchMetalakeException.class, () -> client.alterMetalake(noexists.name(), changes));
  }

  @Test
  public void testCreateMetalake() {
    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeNameA);
    assertEquals(metalakeNameA, metalake.name());
    assertEquals("metalake A comment", metalake.comment());
    assertEquals(AuthConstants.ANONYMOUS_USER, metalake.auditInfo().creator());

    // Test metalake name already exists
    Map<String, String> emptyMap = Collections.emptyMap();
    NameIdentifier exists = NameIdentifier.parse(metalakeNameA);
    assertThrows(
        MetalakeAlreadyExistsException.class,
        () -> {
          client.createMetalake(exists.name(), "metalake A comment", emptyMap);
        });
  }

  @Test
  public void testCreateMetalakeWithChinese() {
    client.createMetalake(metalakeNameA, "这是中文comment", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeNameA);
    assertEquals(metalakeNameA, metalake.name());
    assertEquals("这是中文comment", metalake.comment());
    assertEquals(AuthConstants.ANONYMOUS_USER, metalake.auditInfo().creator());

    // Test metalake name already exists
    Map<String, String> emptyMap = Collections.emptyMap();
    NameIdentifier exists = NameIdentifier.parse(metalakeNameA);
    assertThrows(
        MetalakeAlreadyExistsException.class,
        () -> {
          client.createMetalake(exists.name(), "metalake A comment", emptyMap);
        });
  }

  @Test
  public void testDropMetalakes() {
    GravitinoMetalake metalakeA =
        client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());
    assertTrue(client.dropMetalake(metalakeA.name()), "metaLake should be dropped");
    NameIdentifier id = NameIdentifier.of(metalakeNameA);
    assertThrows(
        NoSuchMetalakeException.class,
        () -> {
          client.loadMetalake(id.name());
        });

    // Metalake does not exist, so we return false
    assertFalse(client.dropMetalake(metalakeA.name()), "metalake should be non-existent");
  }

  public void dropMetalakes() {
    GravitinoMetalake[] metaLakes = client.listMetalakes();
    for (GravitinoMetalake metalake : metaLakes) {
      assertTrue(client.dropMetalake(metalake.name()));
    }

    // Reload metadata from backend to check if the drop operations are applied
    for (GravitinoMetalake metalake : metaLakes) {
      NameIdentifier id = NameIdentifier.of(metalake.name());
      Assertions.assertThrows(
          NoSuchMetalakeException.class,
          () -> {
            client.loadMetalake(id.name());
          });
    }

    for (GravitinoMetalake metalake : metaLakes) {
      Assertions.assertFalse(client.dropMetalake(metalake.name()));
    }
  }
}
