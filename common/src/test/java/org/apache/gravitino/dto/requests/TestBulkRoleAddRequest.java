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
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBulkRoleAddRequest {

  @Test
  public void testBulkRoleAddRequestSerDe() throws JsonProcessingException {
    BulkRoleAddRequest request =
        new BulkRoleAddRequest(
            new RoleCreateRequest[] {
              new RoleCreateRequest(
                  "role1", ImmutableMap.of("key1", "value1"), new SecurableObjectDTO[] {}),
              new RoleCreateRequest("role2", null, new SecurableObjectDTO[] {})
            });

    String serJson = JsonUtils.objectMapper().writeValueAsString(request);
    BulkRoleAddRequest deserRequest =
        JsonUtils.objectMapper().readValue(serJson, BulkRoleAddRequest.class);

    Assertions.assertEquals(request, deserRequest);
    Assertions.assertEquals(2, deserRequest.getRoles().length);
    Assertions.assertEquals("role1", deserRequest.getRoles()[0].getName());
    Assertions.assertEquals(
        ImmutableMap.of("key1", "value1"), deserRequest.getRoles()[0].getProperties());
    Assertions.assertDoesNotThrow(deserRequest::validate);
  }

  @Test
  public void testBulkRoleAddRequestValidate() {
    Assertions.assertThrows(IllegalArgumentException.class, new BulkRoleAddRequest()::validate);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new BulkRoleAddRequest(new RoleCreateRequest[] {}).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new BulkRoleAddRequest(new RoleCreateRequest[] {null}).validate());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new BulkRoleAddRequest(
                    new RoleCreateRequest[] {
                      new RoleCreateRequest("", null, new SecurableObjectDTO[] {})
                    })
                .validate());
  }
}
