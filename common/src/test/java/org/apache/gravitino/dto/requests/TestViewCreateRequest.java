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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewCreateRequest {

  @Test
  public void testViewCreateRequestSerDeAndValidate() throws JsonProcessingException {
    ViewCreateRequest request =
        ViewCreateRequest.builder()
            .name("v1")
            .comment("comment")
            .columns(new ColumnDTO[0])
            .representations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
                })
            .defaultCatalog("cat")
            .defaultSchema("sch")
            .properties(Map.of("k", "v"))
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(request);
    ViewCreateRequest deserialized =
        JsonUtils.objectMapper().readValue(json, ViewCreateRequest.class);

    Assertions.assertEquals("v1", deserialized.getName());
    Assertions.assertEquals("comment", deserialized.getComment());
    Assertions.assertEquals("cat", deserialized.getDefaultCatalog());
    Assertions.assertEquals("sch", deserialized.getDefaultSchema());
    Assertions.assertEquals(Map.of("k", "v"), deserialized.getProperties());
    Assertions.assertDoesNotThrow(deserialized::validate);
  }

  @Test
  public void testViewCreateRequestValidateRequiredFields() {
    ViewCreateRequest missingName =
        ViewCreateRequest.builder()
            .representations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
                })
            .build();
    IllegalArgumentException exception1 =
        Assertions.assertThrows(IllegalArgumentException.class, missingName::validate);
    Assertions.assertEquals(
        "\"name\" field is required and cannot be empty", exception1.getMessage());

    ViewCreateRequest missingRepresentations = ViewCreateRequest.builder().name("v1").build();
    IllegalArgumentException exception2 =
        Assertions.assertThrows(IllegalArgumentException.class, missingRepresentations::validate);
    Assertions.assertEquals(
        "\"representations\" field is required and cannot be empty", exception2.getMessage());
  }

  @Test
  public void testViewCreateRequestValidateDuplicateDialect() {
    ViewCreateRequest duplicateDialect =
        ViewCreateRequest.builder()
            .name("v1")
            .representations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build(),
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 2").build()
                })
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, duplicateDialect::validate);
    Assertions.assertEquals("Duplicate SQL representation dialect: trino", exception.getMessage());
  }

  @Test
  public void testViewCreateRequestValidateNullColumn() {
    ViewCreateRequest request =
        ViewCreateRequest.builder()
            .name("v1")
            .columns(new ColumnDTO[] {null})
            .representations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
                })
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("column must not be null", exception.getMessage());
  }

  @Test
  public void testViewCreateRequestValidateInvalidColumn() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"name\":\"v1\","
            + "\"columns\":[{}],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}]"
            + "}";

    ViewCreateRequest request =
        JsonUtils.objectMapper().readValue(rawJson, ViewCreateRequest.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, request::validate);
    Assertions.assertEquals("Column name cannot be null or empty.", exception.getMessage());
  }

  @Test
  public void testViewCreateRequestDeserializeFromRawJson() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"name\":\"v_raw\","
            + "\"comment\":\"raw comment\","
            + "\"columns\":[],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}],"
            + "\"defaultCatalog\":\"cat\","
            + "\"defaultSchema\":\"sch\","
            + "\"properties\":{\"k\":\"v\"}"
            + "}";

    ViewCreateRequest request =
        JsonUtils.objectMapper().readValue(rawJson, ViewCreateRequest.class);

    Assertions.assertEquals("v_raw", request.getName());
    Assertions.assertEquals("raw comment", request.getComment());
    Assertions.assertEquals("cat", request.getDefaultCatalog());
    Assertions.assertEquals("sch", request.getDefaultSchema());
    Assertions.assertEquals("v", request.getProperties().get("k"));
    Assertions.assertDoesNotThrow(request::validate);
  }
}
