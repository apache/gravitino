/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.dto.file;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetContextDTO {
  @Test
  void testJsonSerDe() throws JsonProcessingException {
    FilesetDTO filesetDTO =
        FilesetDTO.builder()
            .name("test")
            .type(Fileset.Type.MANAGED)
            .audit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .storageLocation("hdfs://host/test")
            .build();

    FilesetContextDTO dto =
        FilesetContextDTO.builder()
            .fileset(filesetDTO)
            .actualPath("hdfs://host/test/1.txt")
            .build();
    String value = JsonUtils.objectMapper().writeValueAsString(dto);

    String expectedValue =
        String.format(
            "{\n"
                + "     \"fileset\": {\n"
                + "          \"name\": \"test\",\n"
                + "          \"comment\": null,\n"
                + "          \"type\": \"managed\",\n"
                + "          \"storageLocation\": \"hdfs://host/test\",\n"
                + "          \"properties\": null,\n"
                + "          \"audit\": {\n"
                + "               \"creator\": \"creator\",\n"
                + "               \"createTime\": \"%s\",\n"
                + "               \"lastModifier\": null,\n"
                + "               \"lastModifiedTime\": null\n"
                + "          }\n"
                + "     },\n"
                + "     \"actualPath\": \"hdfs://host/test/1.txt\"\n"
                + "}",
            filesetDTO.auditInfo().createTime());
    JsonNode expected = JsonUtils.objectMapper().readTree(expectedValue);
    JsonNode actual = JsonUtils.objectMapper().readTree(value);
    Assertions.assertEquals(expected, actual);
  }
}
