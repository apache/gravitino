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
package org.apache.gravitino.dto.file;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetDTO {

  @Test
  public void testFilesetSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();
    Map<String, String> props = ImmutableMap.of("key", "value");

    // test with default location
    FilesetDTO filesetDTO =
        FilesetDTO.builder()
            .name("fileset_test")
            .comment("model comment")
            .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/a/b/c", "v1", "/d/e/f"))
            .properties(props)
            .audit(audit)
            .build();
    Assertions.assertEquals("fileset_test", filesetDTO.name());
    Assertions.assertEquals("model comment", filesetDTO.comment());
    Assertions.assertEquals(
        ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/a/b/c", "v1", "/d/e/f"),
        filesetDTO.storageLocations());
    Assertions.assertEquals("/a/b/c", filesetDTO.storageLocation());
    Assertions.assertEquals(props, filesetDTO.properties());
    Assertions.assertEquals(audit, filesetDTO.auditInfo());

    String serJson = JsonUtils.objectMapper().writeValueAsString(filesetDTO);
    FilesetDTO deserFilesetDTO = JsonUtils.objectMapper().readValue(serJson, FilesetDTO.class);
    Assertions.assertEquals(filesetDTO, deserFilesetDTO);

    // test without default location exception
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> FilesetDTO.builder().name("test").build());
    Assertions.assertEquals(
        "storage locations cannot be empty. At least one location is required.",
        exception.getMessage());

    // test empty location name exception
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> FilesetDTO.builder().storageLocations(ImmutableMap.of("", "/a/b/c")).build());
    Assertions.assertEquals("name cannot be null or empty", exception.getMessage());

    // test empty default location
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FilesetDTO.builder()
                    .name("test")
                    .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, ""))
                    .build());
    Assertions.assertEquals("storage location cannot be empty", exception.getMessage());

    // test empty location
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FilesetDTO.builder()
                    .name("test")
                    .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/a/b/c", "v1", ""))
                    .build());
    Assertions.assertEquals("storage location cannot be empty", exception.getMessage());
  }
}
