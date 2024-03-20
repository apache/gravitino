/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.json;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.responses.BaseResponse;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.responses.MetalakeResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestResponseJsonSerDe {

  @Test
  public void testBaseResponseSerDe() throws JsonProcessingException {
    BaseResponse response = new BaseResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    BaseResponse deserResponse = JsonUtils.objectMapper().readValue(serJson, BaseResponse.class);
    Assertions.assertEquals(response, deserResponse);

    ErrorResponse response1 = ErrorResponse.internalError("internal error");
    String serJson1 = JsonUtils.objectMapper().writeValueAsString(response1);
    ErrorResponse deserResponse1 =
        JsonUtils.objectMapper().readValue(serJson1, ErrorResponse.class);
    Assertions.assertEquals(response1, deserResponse1);
  }

  @Test
  public void testDropResponseSerDe() throws JsonProcessingException {
    DropResponse response = new DropResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    DropResponse deserResponse = JsonUtils.objectMapper().readValue(serJson, DropResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }

  @Test
  public void testEntityListResponseSerDe() throws JsonProcessingException {
    EntityListResponse response = new EntityListResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    EntityListResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, EntityListResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }

  @Test
  public void testMetalakeResponseSerDe() throws JsonProcessingException {
    MetalakeResponse response = new MetalakeResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    MetalakeResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, MetalakeResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }

  @Test
  public void testMetalakeListResponseSerDe() throws JsonProcessingException {
    MetalakeListResponse response = new MetalakeListResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    MetalakeListResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, MetalakeListResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }

  @Test
  public void testCatalogResponseSerDe() throws JsonProcessingException {
    CatalogResponse response = new CatalogResponse();
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    CatalogResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, CatalogResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }

  @Test
  public void testMetalakeResponseBuilderSerDe() throws JsonProcessingException {
    MetalakeDTO metalake =
        MetalakeDTO.builder()
            .withName("metalake")
            .withComment("comment")
            .withProperties(ImmutableMap.of("key", "value"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    MetalakeResponse response = new MetalakeResponse(metalake);
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    MetalakeResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, MetalakeResponse.class);
    Assertions.assertEquals(response, deserResponse);
  }
}
