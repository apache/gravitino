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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import org.apache.gravitino.domain.DefinitionMember;
import org.apache.gravitino.domain.DefinitionMemberPage;
import org.apache.gravitino.dto.domain.DefinitionMemberDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Test;

class TestDefinitionMemberListResponse {

  @Test
  void roundTripsMemberTypesAndCurrentNames() throws IOException {
    DefinitionMemberPage page =
        new DefinitionMemberPage(
            Arrays.asList(
                new DefinitionMemberDTO(DefinitionMember.Type.GLOSSARY_TERM, "共享名称"),
                new DefinitionMemberDTO(DefinitionMember.Type.KNOWLEDGE_BASE_ARTICLE, "共享名称")),
            true);
    ObjectMapper mapper = JsonUtils.objectMapper();
    DefinitionMemberListResponse response =
        mapper.readValue(
            mapper.writeValueAsString(new DefinitionMemberListResponse(page)),
            DefinitionMemberListResponse.class);
    assertEquals(0, response.getCode());
    assertEquals(2, response.getMembers().size());
    assertEquals(DefinitionMember.Type.GLOSSARY_TERM, response.getMembers().get(0).type());
    assertEquals(DefinitionMember.Type.KNOWLEDGE_BASE_ARTICLE, response.getMembers().get(1).type());
    assertEquals("共享名称", response.getMembers().get(0).name());
    assertEquals("共享名称", response.getMembers().get(1).name());
    assertTrue(response.isHasMore());
  }
}
