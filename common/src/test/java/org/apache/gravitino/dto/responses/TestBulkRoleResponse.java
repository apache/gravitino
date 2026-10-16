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
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBulkRoleResponse {

  @Test
  public void testBulkRoleResponseSerDe() throws JsonProcessingException {
    RoleDTO role =
        RoleDTO.builder()
            .withName("role1")
            .withProperties(ImmutableMap.of("key1", "value1"))
            .withSecurableObjects(new SecurableObjectDTO[] {})
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    BulkError error =
        new BulkError(
            1,
            "role2",
            ErrorConstants.ALREADY_EXISTS_CODE,
            "RoleAlreadyExistsException",
            "Role already exists: role2");
    BulkRoleResponse response =
        new BulkRoleResponse(
            new RoleDTO[] {role}, new BulkError[] {error}, new BulkSummary(2, 1, 1));

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    BulkRoleResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, BulkRoleResponse.class);

    Assertions.assertEquals(response.getCode(), deserResponse.getCode());
    Assertions.assertEquals(1, deserResponse.getRoles().length);
    Assertions.assertEquals("role1", deserResponse.getRoles()[0].name());
    Assertions.assertEquals(1, deserResponse.getErrors().length);
    Assertions.assertEquals("role2", deserResponse.getErrors()[0].getName());
    Assertions.assertDoesNotThrow(deserResponse::validate);
  }

  @Test
  public void testBulkRoleResponseValidate() {
    Assertions.assertThrows(IllegalArgumentException.class, new BulkRoleResponse()::validate);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new BulkRoleResponse(null, new BulkError[] {}, new BulkSummary(0, 0, 0)).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new BulkRoleResponse(new RoleDTO[] {}, null, new BulkSummary(0, 0, 0)).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new BulkRoleResponse(new RoleDTO[] {}, new BulkError[] {}, null).validate());
  }
}
