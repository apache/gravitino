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
package org.apache.gravitino.dto.tag;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagDTO {

  @Test
  public void testTagSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    TagDTO tagDTO =
        TagDTO.builder().withName("tag_test").withComment("tag comment").withAudit(audit).build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO);
    TagDTO deserTagDTO = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO, deserTagDTO);

    Assertions.assertEquals("tag_test", deserTagDTO.name());
    Assertions.assertEquals("tag comment", deserTagDTO.comment());
    Assertions.assertEquals(audit, deserTagDTO.auditInfo());
    Assertions.assertNull(deserTagDTO.properties());

    // Test tag with property
    Map<String, String> properties = ImmutableMap.of("key", "value");
    TagDTO tagDTO1 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withProperties(properties)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO1);
    TagDTO deserTagDTO1 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO1, deserTagDTO1);

    Assertions.assertEquals(properties, deserTagDTO1.properties());

    // Test tag with inherited
    TagDTO tagDTO2 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.empty())
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO2);
    TagDTO deserTagDTO2 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO2, deserTagDTO2);
    Assertions.assertEquals(Optional.empty(), deserTagDTO2.inherited());

    TagDTO tagDTO3 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.of(false))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO3);
    TagDTO deserTagDTO3 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(Optional.of(false), deserTagDTO3.inherited());

    TagDTO tagDTO4 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.of(true))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO4);
    TagDTO deserTagDTO4 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(Optional.of(true), deserTagDTO4.inherited());
  }
}
