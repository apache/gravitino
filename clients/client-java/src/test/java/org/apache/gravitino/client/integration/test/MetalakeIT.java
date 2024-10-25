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
package org.apache.gravitino.client.integration.test;

import static org.apache.gravitino.Metalake.PROPERTY_IN_USE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyMetalakeException;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetalakeIT extends BaseIT {
  public static String metalakeNameA = RandomNameUtils.genRandomName("metalakeA");
  public static String metalakeNameB = RandomNameUtils.genRandomName("metalakeB");

  @BeforeEach
  public void start() {
    // Just in case clean up is needed due to a test failure
    dropMetalakes();
  }

  @AfterEach
  public void stop() {
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
    assertEquals("true", metaLakeA.properties().get(PROPERTY_IN_USE));

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
    assertEquals("true", metalake.properties().get(PROPERTY_IN_USE));

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
    assertDoesNotThrow(() -> client.disableMetalake(metalakeA.name()));
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

  @Test
  public void testUpdateMetalakeWithNullableComment() {
    client.createMetalake(metalakeNameA, null, Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeNameA);
    assertEquals(metalakeNameA, metalake.name());
    assertEquals(null, metalake.comment());

    MetalakeChange[] changes =
        new MetalakeChange[] {MetalakeChange.updateComment("new metalake comment")};
    GravitinoMetalake updatedMetalake = client.alterMetalake(metalakeNameA, changes);
    assertEquals("new metalake comment", updatedMetalake.comment());
    assertTrue(client.dropMetalake(metalakeNameA, true));
  }

  @Test
  public void testMetalakeAvailable() {
    String metalakeName = RandomNameUtils.genRandomName("test_metalake");
    client.createMetalake(metalakeName, null, null);
    // test load metalake
    GravitinoMetalake metalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, metalake.name());
    Assertions.assertEquals("true", metalake.properties().get(PROPERTY_IN_USE));

    // test list metalakes
    GravitinoMetalake[] metalakes = client.listMetalakes();
    Assertions.assertEquals(1, metalakes.length);
    Assertions.assertEquals(metalakeName, metalakes[0].name());
    Assertions.assertEquals("true", metalakes[0].properties().get(PROPERTY_IN_USE));

    Exception exception =
        assertThrows(MetalakeInUseException.class, () -> client.dropMetalake(metalakeName));
    Assertions.assertTrue(exception.getMessage().contains("please disable it first"));

    // create a catalog under the metalake
    String catalogName = GravitinoITUtils.genRandomName("test_catalog");
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", ImmutableMap.of());
    Assertions.assertEquals("true", catalog.properties().get(Catalog.PROPERTY_IN_USE));

    Assertions.assertDoesNotThrow(() -> client.disableMetalake(metalakeName));
    GravitinoMetalake loadedMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals("false", loadedMetalake.properties().get(PROPERTY_IN_USE));

    exception =
        assertThrows(
            MetalakeNotInUseException.class,
            () -> client.alterMetalake(metalakeName, MetalakeChange.updateComment("new comment")));
    Assertions.assertTrue(exception.getMessage().contains("please enable it first"));

    // test catalog operations under non-in-use metalake
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () ->
            loadedMetalake.createCatalog(
                catalogName, Catalog.Type.FILESET, "dummy", null, Collections.emptyMap()));
    Assertions.assertThrows(MetalakeNotInUseException.class, loadedMetalake::listCatalogs);
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> loadedMetalake.loadCatalog(catalogName));
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> loadedMetalake.dropCatalog(catalogName));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () -> loadedMetalake.alterCatalog(catalogName, CatalogChange.rename("dummy")));
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> loadedMetalake.disableCatalog(catalogName));
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> loadedMetalake.enableCatalog(catalogName));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () ->
            loadedMetalake.testConnection(
                catalogName, Catalog.Type.FILESET, "dummy", null, Collections.emptyMap()));
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> loadedMetalake.catalogExists(catalogName));

    // test schema operations under non-in-use metalake
    SupportsSchemas schemaOps = catalog.asSchemas();
    Assertions.assertThrows(MetalakeNotInUseException.class, schemaOps::listSchemas);
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> schemaOps.createSchema("dummy", null, null));
    Assertions.assertThrows(MetalakeNotInUseException.class, () -> schemaOps.loadSchema("dummy"));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () -> schemaOps.alterSchema("dummy", SchemaChange.removeProperty("dummy")));
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> schemaOps.dropSchema("dummy", false));

    // test fileset operations under non-in-use catalog
    FilesetCatalog filesetOps = catalog.asFilesetCatalog();
    Assertions.assertThrows(
        MetalakeNotInUseException.class, () -> filesetOps.listFilesets(Namespace.of("dummy")));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () -> filesetOps.loadFileset(NameIdentifier.of("dummy", "dummy")));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () ->
            filesetOps.createFileset(NameIdentifier.of("dummy", "dummy"), null, null, null, null));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () -> filesetOps.dropFileset(NameIdentifier.of("dummy", "dummy")));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () -> filesetOps.getFileLocation(NameIdentifier.of("dummy", "dummy"), "dummy"));
    Assertions.assertThrows(
        MetalakeNotInUseException.class,
        () ->
            filesetOps.alterFileset(
                NameIdentifier.of("dummy", "dummy"), FilesetChange.removeComment()));

    Assertions.assertThrows(
        NonEmptyMetalakeException.class, () -> client.dropMetalake(metalakeName));

    Assertions.assertDoesNotThrow(() -> client.enableMetalake(metalakeName));
    Assertions.assertTrue(loadedMetalake.dropCatalog(catalogName, true));

    Assertions.assertDoesNotThrow(() -> client.disableMetalake(metalakeName));
    Assertions.assertTrue(client.dropMetalake(metalakeName));
    Assertions.assertFalse(client.dropMetalake(metalakeName));
  }

  public void dropMetalakes() {
    GravitinoMetalake[] metaLakes = client.listMetalakes();
    for (GravitinoMetalake metalake : metaLakes) {
      assertDoesNotThrow(() -> client.disableMetalake(metalake.name()));
      assertTrue(client.dropMetalake(metalake.name(), true));
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
