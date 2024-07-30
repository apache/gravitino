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

package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.file.ClientType;
import org.apache.gravitino.file.FilesetDataOperation;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGetFilesetContextRequest {
  @Test
  public void testGetFilesetContextRequest() throws JsonProcessingException {
    GetFilesetContextRequest request =
        GetFilesetContextRequest.builder()
            .subPath("/test/1.txt")
            .operation(FilesetDataOperation.CREATE)
            .clientType(ClientType.HADOOP_GVFS)
            .build();
    String jsonString = JsonUtils.objectMapper().writeValueAsString(request);
    String expected =
        "{\"subPath\":\"/test/1.txt\",\"operation\":\"create\",\"clientType\":\"hadoop_gvfs\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(jsonString));
  }
}
