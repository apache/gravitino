/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.exceptions.IllegalNameIdentifierException;
import com.datastrato.graviton.exceptions.IllegalNamespaceException;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.GravitonITUtils;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeIT extends AbstractIT {
  public static String metalakeNameA = GravitonITUtils.genRandomName("metalakeA");
  public static String metalakeNameB = GravitonITUtils.genRandomName("metalakeB");

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
    GravitonMetaLake[] metaLakes = client.listMetalakes();
    assertEquals(0, metaLakes.length);

    // one metalakes
    client.createMetalake(
        NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());
    metaLakes = client.listMetalakes();
    assertEquals(1, metaLakes.length);
    assertEquals(metaLakes[0].name(), metalakeNameA);

    // two metalakes
    client.createMetalake(
        NameIdentifier.parse(metalakeNameB), "metalake B comment", Collections.emptyMap());
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
    client.createMetalake(
        NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());
    GravitonMetaLake metaLakeA = client.loadMetalake(NameIdentifier.of(metalakeNameA));
    assertEquals(metaLakeA.name(), metalakeNameA);

    // metalake does not exist
    assertThrows(
        NoSuchMetalakeException.class, () -> client.loadMetalake(NameIdentifier.of(metalakeNameB)));

    // metalake empty name
    assertThrows(
        IllegalNameIdentifierException.class, () -> client.loadMetalake(NameIdentifier.of("")));

    // metalake bad name
    assertThrows(
        IllegalNamespaceException.class,
        () -> client.loadMetalake(NameIdentifier.of("A", "B", "C")));
  }

  @Test
  public void testAlterMetalake() {
    String newName = GravitonITUtils.genRandomName("newmetaname");

    client.createMetalake(
        NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());

    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
        };
    GravitonMetaLake metaLake = client.alterMetalake(NameIdentifier.of(metalakeNameA), changes);
    assertEquals(newName, metaLake.name());
    assertEquals("new metalake comment", metaLake.comment());
    assertEquals("graviton", metaLake.auditInfo().creator());

    // Reload metatdata via new name to check if the changes are applied
    GravitonMetaLake newMetalake = client.loadMetalake(NameIdentifier.of(newName));
    assertEquals(newName, newMetalake.name());
    assertEquals("new metalake comment", newMetalake.comment());

    // Old name does not exist
    assertThrows(
        NoSuchMetalakeException.class, () -> client.loadMetalake(NameIdentifier.of(metalakeNameA)));
  }

  @Test
  public void testAlterNonExistantMetalake() {
    String newName = GravitonITUtils.genRandomName("newmetaname");

    client.createMetalake(
        NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());

    MetalakeChange[] changes =
        new MetalakeChange[] {
          MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
        };

    // rename non existent metalake
    assertThrows(
        NoSuchMetalakeException.class,
        () -> client.alterMetalake(NameIdentifier.of(metalakeNameB), changes));
  }

  @Test
  public void testCreateMetalake() {
    GravitonMetaLake metaLakeA =
        client.createMetalake(
            NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());
    GravitonMetaLake metalake = client.loadMetalake(NameIdentifier.of(metalakeNameA));
    assertEquals(metalakeNameA, metalake.name());
    assertEquals("metalake A comment", metalake.comment());
    assertEquals("graviton", metalake.auditInfo().creator());

    // Test metalake name already exists
    assertThrows(
        MetalakeAlreadyExistsException.class,
        () ->
            client.createMetalake(
                NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap()));
  }

  @Test
  public void testDropMetalakes() {
    GravitonMetaLake metalakeA =
        client.createMetalake(
            NameIdentifier.parse(metalakeNameA), "metalake A comment", Collections.emptyMap());
    assertTrue(client.dropMetalake(NameIdentifier.of(metalakeA.name())));
    assertThrows(
        NoSuchMetalakeException.class, () -> client.loadMetalake(NameIdentifier.of(metalakeNameA)));

    // Metalake does not exist
    // TODO This should return False not True
    assertTrue(client.dropMetalake(NameIdentifier.of(metalakeA.name())));

    // Bad metalake name
    assertThrows(
        IllegalNamespaceException.class, () -> client.dropMetalake(NameIdentifier.of("A", "B")));
  }

  public void dropMetalakes() {
    GravitonMetaLake[] metaLakes = client.listMetalakes();
    for (GravitonMetaLake metalake : metaLakes) {
      assertTrue(client.dropMetalake(NameIdentifier.of(metalake.name())));
    }
  }
}
