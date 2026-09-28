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
package org.apache.gravitino.dto.authorization;

import java.io.IOException;
import java.util.Collections;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRoleDTO {

  @Test
  public void testSecurableObjectsAbsentInJsonDoesNotThrow() throws IOException {
    // Jackson deserialization bypasses the builder's null check, so a payload without the
    // "securableObjects" field left the array null and Arrays.asList(null) threw an NPE.
    RoleDTO roleDTO =
        JsonUtils.objectMapper()
            .readValue(
                "{\"name\":\"role1\",\"audit\":{\"creator\":\"a\",\"createTime\":\"2024-01-01T00:00:00Z\"}}",
                RoleDTO.class);

    Assertions.assertEquals("role1", roleDTO.name());
    Assertions.assertEquals(Collections.emptyList(), roleDTO.securableObjects());
  }
}
