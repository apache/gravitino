/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.e2e;

import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.DTOConverters;
import com.datastrato.graviton.client.ErrorHandlers;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.MetalakeCreateRequest;
import com.datastrato.graviton.dto.requests.MetalakeUpdatesRequest;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.dto.responses.MetalakeListResponse;
import com.datastrato.graviton.dto.responses.MetalakeResponse;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.integration.util.AbstractIT;
import com.datastrato.graviton.integration.util.GravitonITUtils;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeIT extends AbstractIT {
  public static String metalakeName_RESTful = GravitonITUtils.genRandomName();
  public static String metalakeName_API = GravitonITUtils.genRandomName();

  static String reqPath = "api/metalakes";

  static Consumer<ErrorResponse> onError = ErrorHandlers.restErrorHandler();

  public static MetalakeResponse createDefMetalake(String metalakeName) {
    MetalakeCreateRequest reqBody =
        new MetalakeCreateRequest(metalakeName, "comment", ImmutableMap.of("key", "value"));

    MetalakeResponse successResponse =
        doExecuteRequest(Method.POST, reqPath, reqBody, MetalakeResponse.class, onError, h -> {});
    return successResponse;
  }

  @BeforeAll
  private static void startup() {
    MetalakeResponse successResponse = createDefMetalake(metalakeName_RESTful);
    Assertions.assertEquals(successResponse.getMetalake().name(), metalakeName_RESTful);
  }

  @AfterAll
  private static void teardown() throws IOException {
    DropResponse response =
        doExecuteRequest(
            Method.DELETE,
            reqPath + File.separator + metalakeName_RESTful,
            null,
            DropResponse.class,
            onError,
            h -> {});
    Assertions.assertEquals(response.dropped(), true);
  }

  @Order(1)
  @Test
  public void testListMetalakeRestful() {
    MetalakeListResponse listResponse =
        doExecuteRequest(Method.GET, reqPath, null, MetalakeListResponse.class, onError, h -> {});

    List<MetalakeDTO> result =
        Arrays.stream(listResponse.getMetalakes())
            .filter(metalakeDTO -> metalakeDTO.name().equals(metalakeName_RESTful))
            .collect(Collectors.toList());

    Assertions.assertEquals(result.size(), 1);

    Assertions.assertEquals(result.get(0).name(), metalakeName_RESTful);
    Assertions.assertEquals("comment", result.get(0).comment());
  }

  @Order(2)
  @Test
  public void testPutMetalakeRestful() {
    String putMetalakeName = GravitonITUtils.genRandomName();
    String putReqPath = reqPath + File.separator + metalakeName_RESTful;
    MetalakeChange[] changes1 =
        new MetalakeChange[] {
          MetalakeChange.rename(putMetalakeName), MetalakeChange.updateComment("newComment")
        };

    MetalakeUpdatesRequest reqUpdates1 =
        new MetalakeUpdatesRequest(
            Arrays.stream(changes1)
                .map(DTOConverters::toMetalakeUpdateRequest)
                .collect(Collectors.toList()));

    MetalakeResponse responseUpdates =
        doExecuteRequest(
            Method.PUT, putReqPath, reqUpdates1, MetalakeResponse.class, onError, h -> {});
    Assertions.assertEquals(responseUpdates.getMetalake().name(), putMetalakeName);
    Assertions.assertEquals("newComment", responseUpdates.getMetalake().comment());

    // Restore test record
    putReqPath = reqPath + File.separator + putMetalakeName;
    MetalakeChange[] changes2 = new MetalakeChange[] {MetalakeChange.rename(metalakeName_RESTful)};

    MetalakeUpdatesRequest reqUpdates2 =
        new MetalakeUpdatesRequest(
            Arrays.stream(changes2)
                .map(DTOConverters::toMetalakeUpdateRequest)
                .collect(Collectors.toList()));

    doExecuteRequest(Method.PUT, putReqPath, reqUpdates2, MetalakeResponse.class, onError, h -> {});
  }

  @Order(4)
  @Test
  public void testCreateMetalakeAPI() {
    GravitonMetaLake metaLake =
        client.createMetalake(
            NameIdentifier.parse(metalakeName_API), "comment", Collections.emptyMap());
    Assertions.assertEquals(metalakeName_API, metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Test metalake name already exists
    Throwable excep =
        Assertions.assertThrows(
            MetalakeAlreadyExistsException.class,
            () ->
                client.createMetalake(
                    NameIdentifier.parse(metalakeName_API), "comment", Collections.emptyMap()));
    Assertions.assertTrue(excep.getMessage().contains("already exists"));
  }

  @Order(5)
  @Test
  public void testListMetalakeAPI() {
    GravitonMetaLake[] metaLakes = client.listMetalakes();
    List<MetalakeDTO> result =
        Arrays.stream(metaLakes)
            .filter(metalakeDTO -> metalakeDTO.name().equals(metalakeName_API))
            .collect(Collectors.toList());

    Assertions.assertEquals(result.size(), 1);
  }

  @Order(6)
  @Test
  public void testLoadMetalakeAPI() {
    GravitonMetaLake metaLake = client.loadMetalake(NameIdentifier.of(metalakeName_API));
    Assertions.assertEquals(metaLake.name(), metalakeName_API);
  }

  @Order(7)
  @Test
  public void testAlterMetalakeAPI() {
    String alterMetalakeName = GravitonITUtils.genRandomName();

    MetalakeChange[] changes1 =
        new MetalakeChange[] {
          MetalakeChange.rename(alterMetalakeName), MetalakeChange.updateComment("newComment")
        };
    GravitonMetaLake metaLake = client.alterMetalake(NameIdentifier.of(metalakeName_API), changes1);
    Assertions.assertEquals(alterMetalakeName, metaLake.name());
    Assertions.assertEquals("newComment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Test return not found
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> client.alterMetalake(NameIdentifier.of(metalakeName_API + "mock"), changes1));
    Assertions.assertTrue(excep.getMessage().contains("does not exist"));

    // Restore test record
    MetalakeChange[] changes2 = new MetalakeChange[] {MetalakeChange.rename(metalakeName_API)};
    client.alterMetalake(NameIdentifier.of(alterMetalakeName), changes2);
  }

  @Order(8)
  @Test
  public void testDropMetalakeAPI() {
    Assertions.assertTrue(client.dropMetalake(NameIdentifier.of(metalakeName_API)));

    // Test illegal metalake name identifier
    Throwable excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                client.dropMetalake(
                    NameIdentifier.parse(metalakeName_API + "." + metalakeName_API)));
    Assertions.assertTrue(excep1.getMessage().contains("namespace should be empty"));
  }
}
