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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakeIT extends AbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(MetalakeIT.class);

  public static String newMetalakeNameRESTful = GravitonITUtils.genRandomName();
  public static String newMetalakeNameAPI = GravitonITUtils.genRandomName();

  static String reqPath = "api/metalakes";

  static Consumer<ErrorResponse> onError = ErrorHandlers.restErrorHandler();

  @Order(1)
  @Test
  public void testCreateMetalakeRestful() {
    MetalakeCreateRequest reqBody =
        new MetalakeCreateRequest(
            newMetalakeNameRESTful, "comment", ImmutableMap.of("key", "value"));

    MetalakeResponse successResponse =
        doExecuteRequest(Method.POST, reqPath, reqBody, MetalakeResponse.class, onError, h -> {});
    LOG.info(successResponse.toString());
    Assertions.assertEquals(successResponse.getMetalake().name(), newMetalakeNameRESTful);
    Assertions.assertEquals("comment", successResponse.getMetalake().comment());
  }

  @Order(2)
  @Test
  public void testListMetalakeRestful() {
    MetalakeListResponse listResponse =
        doExecuteRequest(Method.GET, reqPath, null, MetalakeListResponse.class, onError, h -> {});

    List<MetalakeDTO> result =
        Arrays.stream(listResponse.getMetalakes())
            .filter(metalakeDTO -> metalakeDTO.name().equals(newMetalakeNameRESTful))
            .collect(Collectors.toList());

    Assertions.assertEquals(result.size(), 1);

    Assertions.assertEquals(result.get(0).name(), newMetalakeNameRESTful);
    Assertions.assertEquals("comment", result.get(0).comment());
  }

  @Order(3)
  @Test
  public void testPutMetalakeRestful() {
    String putMetalakeName = GravitonITUtils.genRandomName();
    String putReqPath = reqPath + File.separator + newMetalakeNameRESTful;
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
    MetalakeChange[] changes2 =
        new MetalakeChange[] {MetalakeChange.rename(newMetalakeNameRESTful)};

    MetalakeUpdatesRequest reqUpdates2 =
        new MetalakeUpdatesRequest(
            Arrays.stream(changes2)
                .map(DTOConverters::toMetalakeUpdateRequest)
                .collect(Collectors.toList()));

    doExecuteRequest(Method.PUT, putReqPath, reqUpdates2, MetalakeResponse.class, onError, h -> {});
  }

  @Order(4)
  @Test
  public void testDropMetalakeRestful() {
    DropResponse response =
        doExecuteRequest(
            Method.DELETE,
            reqPath + File.separator + newMetalakeNameRESTful,
            null,
            DropResponse.class,
            onError,
            h -> {});
    Assertions.assertEquals(response.dropped(), true);
  }

  @Order(5)
  @Test
  public void testCreateMetalakeAPI() {
    GravitonMetaLake metaLake =
        client.createMetalake(
            NameIdentifier.parse(newMetalakeNameAPI), "comment", Collections.emptyMap());
    Assertions.assertEquals(newMetalakeNameAPI, metaLake.name());
    Assertions.assertEquals("comment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Test metalake name already exists
    Throwable excep =
        Assertions.assertThrows(
            MetalakeAlreadyExistsException.class,
            () ->
                client.createMetalake(
                    NameIdentifier.parse(newMetalakeNameAPI), "comment", Collections.emptyMap()));
    Assertions.assertTrue(excep.getMessage().contains("already exists"));
  }

  @Order(6)
  @Test
  public void testListMetalakeAPI() {
    GravitonMetaLake[] metaLakes = client.listMetalakes();
    List<MetalakeDTO> result =
        Arrays.stream(metaLakes)
            .filter(metalakeDTO -> metalakeDTO.name().equals(newMetalakeNameAPI))
            .collect(Collectors.toList());

    Assertions.assertEquals(result.size(), 1);
  }

  @Order(7)
  @Test
  public void testLoadMetalakeAPI() {
    GravitonMetaLake metaLake = client.loadMetalake(NameIdentifier.of(newMetalakeNameAPI));
    Assertions.assertEquals(metaLake.name(), newMetalakeNameAPI);
  }

  @Order(8)
  @Test
  public void testAlterMetalakeAPI() {
    String alterMetalakeName = GravitonITUtils.genRandomName();

    MetalakeChange[] changes1 =
        new MetalakeChange[] {
          MetalakeChange.rename(alterMetalakeName), MetalakeChange.updateComment("newComment")
        };
    GravitonMetaLake metaLake =
        client.alterMetalake(NameIdentifier.of(newMetalakeNameAPI), changes1);
    Assertions.assertEquals(alterMetalakeName, metaLake.name());
    Assertions.assertEquals("newComment", metaLake.comment());
    Assertions.assertEquals("graviton", metaLake.auditInfo().creator());

    // Test return not found
    Throwable excep =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () -> client.alterMetalake(NameIdentifier.of(newMetalakeNameAPI + "mock"), changes1));
    Assertions.assertTrue(excep.getMessage().contains("does not exist"));

    // Restore test record
    MetalakeChange[] changes2 = new MetalakeChange[] {MetalakeChange.rename(newMetalakeNameAPI)};
    client.alterMetalake(NameIdentifier.of(alterMetalakeName), changes2);
  }

  @Order(9)
  @Test
  public void testDropMetalakeAPI() {
    Assertions.assertTrue(client.dropMetalake(NameIdentifier.of(newMetalakeNameAPI)));

    // Test illegal metalake name identifier
    Throwable excep1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                client.dropMetalake(
                    NameIdentifier.parse(newMetalakeNameAPI + "." + newMetalakeNameAPI)));
    Assertions.assertTrue(excep1.getMessage().contains("namespace should be empty"));
  }
}
