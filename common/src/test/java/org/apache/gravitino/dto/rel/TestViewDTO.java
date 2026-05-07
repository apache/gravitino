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
package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewDTO {

  @Test
  public void testViewDTOSerDe() throws JsonProcessingException {
    ViewDTO dto =
        ViewDTO.builder()
            .withName("v1")
            .withComment("comment")
            .withColumns(
                new ColumnDTO[] {
                  ColumnDTO.builder().withName("c1").withDataType(Types.LongType.get()).build()
                })
            .withRepresentations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
                })
            .withDefaultCatalog("cat")
            .withDefaultSchema("sch")
            .withProperties(Map.of("k", "v"))
            .withAudit(audit())
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(dto);
    ViewDTO deserialized = JsonUtils.objectMapper().readValue(json, ViewDTO.class);

    Assertions.assertEquals("v1", deserialized.name());
    Assertions.assertEquals("comment", deserialized.comment());
    Assertions.assertEquals(1, deserialized.columns().length);
    Assertions.assertEquals("cat", deserialized.defaultCatalog());
    Assertions.assertEquals("sch", deserialized.defaultSchema());
    Assertions.assertEquals(Map.of("k", "v"), deserialized.properties());
    Assertions.assertEquals(1, deserialized.representations().length);
    Assertions.assertInstanceOf(SQLRepresentationDTO.class, deserialized.representations()[0]);
  }

  @Test
  public void testViewDTOBuildDefaultColumnsAndProperties() {
    ViewDTO dto =
        ViewDTO.builder()
            .withName("v1")
            .withRepresentations(
                new RepresentationDTO[] {
                  SQLRepresentationDTO.builder().withDialect("spark").withSql("SELECT 2").build()
                })
            .withAudit(audit())
            .build();

    Assertions.assertEquals(0, dto.columns().length);
    Assertions.assertTrue(dto.properties().isEmpty());
  }

  @Test
  public void testViewDTOBuildValidation() {
    IllegalArgumentException exception1 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ViewDTO.builder()
                    .withAudit(audit())
                    .withRepresentations(new RepresentationDTO[0])
                    .build());
    Assertions.assertEquals("name cannot be null or empty", exception1.getMessage());

    IllegalArgumentException exception2 =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ViewDTO.builder()
                    .withName("v1")
                    .withRepresentations(new RepresentationDTO[0])
                    .build());
    Assertions.assertEquals("audit cannot be null", exception2.getMessage());
  }

  @Test
  public void testViewDTODeserializeFromRawJson() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"name\":\"v_raw\","
            + "\"comment\":\"raw comment\","
            + "\"columns\":[],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"spark\",\"sql\":\"SELECT 2\"}],"
            + "\"defaultCatalog\":\"cat\","
            + "\"defaultSchema\":\"sch\","
            + "\"properties\":{\"k\":\"v\"},"
            + "\"audit\":{}"
            + "}";

    ViewDTO dto = JsonUtils.objectMapper().readValue(rawJson, ViewDTO.class);

    Assertions.assertEquals("v_raw", dto.name());
    Assertions.assertEquals("raw comment", dto.comment());
    Assertions.assertEquals(1, dto.representations().length);
    Assertions.assertInstanceOf(SQLRepresentationDTO.class, dto.representations()[0]);
    Assertions.assertEquals("cat", dto.defaultCatalog());
    Assertions.assertEquals("sch", dto.defaultSchema());
    Assertions.assertEquals("v", dto.properties().get("k"));
  }

  private AuditDTO audit() {
    return AuditDTO.builder().withCreator("u1").withCreateTime(Instant.now()).build();
  }
}
