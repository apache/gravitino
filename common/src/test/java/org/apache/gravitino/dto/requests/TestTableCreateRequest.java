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
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableCreateRequest {

  @Test
  public void testDeserializeTableCreateRequestWithIndexProperties()
      throws JsonProcessingException {
    String rawJson =
        "{"
            + "\"name\":\"table1\","
            + "\"comment\":\"mock comment\","
            + "\"columns\":["
            + "{\"name\":\"col1\",\"type\":\"string\",\"comment\":\"comment1\",\"nullable\":true,\"autoIncrement\":false},"
            + "{\"name\":\"col2\",\"type\":\"byte\",\"comment\":\"comment2\",\"nullable\":true,\"autoIncrement\":false}"
            + "],"
            + "\"properties\":{\"k1\":\"v1\"},"
            + "\"indexes\":["
            + "{\"indexType\":\"PRIMARY_KEY\",\"name\":\"idx_col1\",\"fieldNames\":[[\"col1\"]],"
            + "\"properties\":{\"granularity\":\"9\"}}"
            + "]"
            + "}";

    TableCreateRequest request =
        JsonUtils.objectMapper().readValue(rawJson, TableCreateRequest.class);
    Assertions.assertEquals("table1", request.getName());
    Assertions.assertNotNull(request.getIndexes());
    Assertions.assertEquals(1, request.getIndexes().length);

    IndexDTO index = request.getIndexes()[0];
    Assertions.assertEquals(Index.IndexType.PRIMARY_KEY, index.type());
    Assertions.assertEquals("idx_col1", index.name());
    Assertions.assertEquals(1, index.fieldNames().length);
    Assertions.assertArrayEquals(new String[] {"col1"}, index.fieldNames()[0]);
    Assertions.assertEquals("9", index.properties().get("granularity"));
  }
}
