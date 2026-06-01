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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewResponse {

  @Test
  public void testViewResponseSerDeAndValidate() throws JsonProcessingException {
    ViewResponse response = new ViewResponse(viewDTO("v1"));

    String json = JsonUtils.objectMapper().writeValueAsString(response);
    ViewResponse deserialized = JsonUtils.objectMapper().readValue(json, ViewResponse.class);

    Assertions.assertEquals("v1", deserialized.getView().name());
    Assertions.assertDoesNotThrow(deserialized::validate);
  }

  @Test
  public void testViewResponseValidateWithNullView() {
    ViewResponse response = new ViewResponse();
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, response::validate);
    Assertions.assertEquals("view must not be null", exception.getMessage());
  }

  @Test
  public void testViewResponseValidateWithMissingRepresentations() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"code\":0,"
            + "\"view\":{"
            + "\"name\":\"v1\","
            + "\"columns\":[],"
            + "\"representations\":[],"
            + "\"audit\":{}"
            + "}"
            + "}";
    ViewResponse response = JsonUtils.objectMapper().readValue(rawJson, ViewResponse.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, response::validate);
    Assertions.assertEquals(
        "view 'representations' must not be null or empty", exception.getMessage());
  }

  @Test
  public void testViewResponseValidateWithMissingNameMessage() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"code\":0,"
            + "\"view\":{"
            + "\"name\":\"\","
            + "\"columns\":[],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}],"
            + "\"audit\":{}"
            + "}"
            + "}";

    ViewResponse malformed = JsonUtils.objectMapper().readValue(rawJson, ViewResponse.class);

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, malformed::validate);
    Assertions.assertEquals("view 'name' must not be null or empty", exception.getMessage());
  }

  @Test
  public void testViewResponseDeserializeFromRawJson() throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"code\":0,"
            + "\"view\":{"
            + "\"name\":\"v1\","
            + "\"columns\":[],"
            + "\"representations\":[{\"type\":\"sql\",\"dialect\":\"trino\",\"sql\":\"SELECT 1\"}],"
            + "\"audit\":{}"
            + "}"
            + "}";

    ViewResponse response = JsonUtils.objectMapper().readValue(rawJson, ViewResponse.class);

    Assertions.assertEquals("v1", response.getView().name());
    Assertions.assertEquals(1, response.getView().representations().length);
    Assertions.assertDoesNotThrow(response::validate);
  }

  private ViewDTO viewDTO(String name) {
    return ViewDTO.builder()
        .withName(name)
        .withColumns(
            new ColumnDTO[] {
              ColumnDTO.builder().withName("c1").withDataType(Types.IntegerType.get()).build()
            })
        .withRepresentations(
            new RepresentationDTO[] {
              SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build()
            })
        .withAudit(audit())
        .build();
  }

  private AuditDTO audit() {
    return AuditDTO.builder().withCreator("u1").withCreateTime(Instant.now()).build();
  }
}
