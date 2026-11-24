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
package org.apache.gravitino.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyExecutionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExecutablePolicyContentSerde {

  private static final ObjectMapper MAPPER = JsonUtils.anyFieldMapper();

  @Test
  public void testReadExecutionSpecWithOptions() throws Exception {
    JsonNode node =
        MAPPER.readTree(
            "{\"timeTrigger\":\"0 0 * * *\",\"commitTrigger\":true,\"minIntervalMs\":60000,"
                + "\"jobOptions\":{\"spark.executor.memory\":\"4g\",\"spark.driver.memory\":\"2g\"},"
                + "\"updaterOptions\":{\"gravitino.uri\":\"http://localhost:8090\"}}");

    PolicyExecutionSpec spec = ExecutablePolicyContentSerde.readExecutionSpec(node);

    Assertions.assertEquals("0 0 * * *", spec.timeTrigger());
    Assertions.assertTrue(spec.commitTrigger());
    Assertions.assertEquals(60000L, spec.minIntervalMs());
    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g", "spark.driver.memory", "2g"),
        spec.jobOptions());
    Assertions.assertEquals(
        ImmutableList.of("spark.executor.memory", "spark.driver.memory"),
        ImmutableList.copyOf(spec.jobOptions().keySet()));
    Assertions.assertEquals(
        ImmutableMap.of("gravitino.uri", "http://localhost:8090"), spec.updaterOptions());
  }

  @Test
  public void testReadExecutionSpecWithAbsentOrNullOptions() throws Exception {
    PolicyExecutionSpec spec =
        ExecutablePolicyContentSerde.readExecutionSpec(
            MAPPER.readTree("{\"jobOptions\":null,\"minIntervalMs\":10}"));

    Assertions.assertTrue(spec.jobOptions().isEmpty());
    Assertions.assertTrue(spec.updaterOptions().isEmpty());
    Assertions.assertEquals(10L, spec.minIntervalMs());
  }

  @Test
  public void testReadExecutionSpecRejectsNonObjectOptions() throws Exception {
    JsonNode node = MAPPER.readTree("{\"jobOptions\":[\"spark.executor.memory\"]}");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ExecutablePolicyContentSerde.readExecutionSpec(node));
    Assertions.assertTrue(
        exception.getMessage().contains("Cannot parse string map property jobOptions"),
        exception.getMessage());
  }

  @Test
  public void testCompactionContentRoundTripWithOptions() throws Exception {
    Map<String, String> rewriteOptions = new LinkedHashMap<>();
    rewriteOptions.put("target-file-size-bytes", "536870912");
    rewriteOptions.put("min-input-files", "5");
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L,
                1L,
                1L,
                100L,
                50L,
                IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                rewriteOptions,
                "0 0 * * *",
                false,
                60000L,
                ImmutableMap.of("spark.executor.memory", "4g"),
                ImmutableMap.of("gravitino.uri", "http://localhost:8090"));

    String json = MAPPER.writeValueAsString(content);
    IcebergDataCompactionContent deserialized =
        MAPPER.readValue(json, IcebergDataCompactionContent.class);

    Assertions.assertEquals(content, deserialized);
    Assertions.assertEquals(rewriteOptions, deserialized.rewriteOptions());
    Assertions.assertEquals(
        ImmutableList.of("target-file-size-bytes", "min-input-files"),
        ImmutableList.copyOf(deserialized.rewriteOptions().keySet()));
    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g"), deserialized.executionSpec().jobOptions());
    Assertions.assertEquals(
        ImmutableMap.of("gravitino.uri", "http://localhost:8090"),
        deserialized.executionSpec().updaterOptions());
  }
}
