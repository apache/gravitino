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
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRepresentationDTO {

  @Test
  public void testRepresentationDTOSerDe() throws JsonProcessingException {
    SQLRepresentationDTO sqlRepresentationDTO =
        SQLRepresentationDTO.fromSQLRepresentation(
            SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build());

    String json = JsonUtils.objectMapper().writeValueAsString(sqlRepresentationDTO);
    RepresentationDTO deserialized =
        JsonUtils.objectMapper().readValue(json, RepresentationDTO.class);

    Assertions.assertInstanceOf(SQLRepresentationDTO.class, deserialized);
    Assertions.assertEquals(Representation.TYPE_SQL, deserialized.type());
    Assertions.assertEquals("spark", ((SQLRepresentationDTO) deserialized).getDialect());
    Assertions.assertEquals("SELECT 1", ((SQLRepresentationDTO) deserialized).getSql());
  }

  @Test
  public void testRepresentationDTOConversion() {
    Representation representation =
        SQLRepresentation.builder().withDialect("trino").withSql("SELECT c1 FROM t").build();

    RepresentationDTO dto = RepresentationDTO.fromRepresentation(representation);
    Representation converted = dto.toRepresentation();

    Assertions.assertEquals(representation, converted);
  }
}
