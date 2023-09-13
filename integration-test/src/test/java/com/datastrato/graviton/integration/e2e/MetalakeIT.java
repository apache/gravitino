/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.GravitonITUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeIT extends AbstractIT {
  public static String metalakeName = GravitonITUtils.genRandomName("metalake");

  @BeforeAll
  private static void start() {
    // Prepare create a metalake test record,
    // This way it can support `Run all tests` or `Run test in separated` to test `list`, `load`,
    // `alter`, `drop` methods.
    createMetalake();
  }

  @AfterAll
  private static void stop() {
    // Always drop the test record here.
    // This allows metalake to be safe deleted metalake when `Run all tests` or `Run test in
    // separated`
    dropMetalake();
  }

  @Order(1)
  @Test
  public void testListMetalake() {
    GravitonMetaLake[] metaLakes = client.listMetalakes();
    List<MetalakeDTO> result =
        Arrays.stream(metaLakes)
            .filter(metalakeDTO -> metalakeDTO.name().equals(metalakeName))
            .collect(Collectors.toList());

    Assertions.assertEquals(result.size(), 1);
  }

  @Order(2)
  @Test
  public void testLoadMetalake() {
    GravitonMetaLake metaLake = client.loadMetalake(NameIdentifier.of(metalakeName));
    Assertions.assertEquals(metaLake.name(), metalakeName);
  }

  @Order(3)
  @Test
  public void testAlterMetalake() {
    String alterMetalakeName = GravitonITUtils.genRandomName("metalake");

    // TODO: Add more test cases for alter metalake
    MetalakeChange[] changes1 =
        new MetalakeChange[] {
          MetalakeChange.rename(alterMetalakeName), MetalakeChange.updateComment("newComment")
        };
    GravitonMetaLake metaLake = client.alterMetalake(NameIdentifier.of(metalakeName), changes1);
    Assertions.assertEquals(alterMetalakeName, metaLake.name());
    Assertions.assertEquals("newComment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Reload metatada from backend to check if the changes are applied
    GravitonMetaLake metaLake1 = client.loadMetalake(NameIdentifier.of(alterMetalakeName));
    Assertions.assertEquals(alterMetalakeName, metaLake1.name());
    Assertions.assertEquals("newComment", metaLake1.comment());
    Assertions.assertEquals("graviton", metaLake1.auditInfo().creator());

    // Test return not found
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> client.alterMetalake(NameIdentifier.of(metalakeName + "mock"), changes1));
    Assertions.assertTrue(excep.getMessage().contains("does not exist"));

    // Restore test record
    MetalakeChange[] changes2 = new MetalakeChange[] {MetalakeChange.rename(metalakeName)};
    client.alterMetalake(NameIdentifier.of(alterMetalakeName), changes2);
  }

  public static void createMetalake() {
    GravitonMetaLake metaLake =
        client.createMetalake(
            NameIdentifier.parse(metalakeName), "comment", Collections.emptyMap());
    Assertions.assertEquals(metalakeName, metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Test metalake name already exists
    Throwable excep =
        Assertions.assertThrows(
            MetalakeAlreadyExistsException.class,
            () ->
                client.createMetalake(
                    NameIdentifier.parse(metalakeName), "comment", Collections.emptyMap()));
    Assertions.assertTrue(excep.getMessage().contains("already exists"));
  }

  public static void dropMetalake() {
    Assertions.assertTrue(client.dropMetalake(NameIdentifier.of(metalakeName)));

    // Reload metatada from backend to check if the drop are applied
    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> client.loadMetalake(NameIdentifier.of(metalakeName)));

    // Return false if the metalake has been dropped already
    Assertions.assertFalse(client.dropMetalake(NameIdentifier.of(metalakeName)));
  }
}
