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
package org.apache.gravitino.dto.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobDTO {

  @Test
  public void testSerDeWithFinishedAt() throws JsonProcessingException {
    Instant finishedAt = Instant.now();
    JobDTO jobDTO =
        new JobDTO(
            "job-123",
            "testTemplate",
            JobHandle.Status.SUCCEEDED,
            AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build(),
            finishedAt);

    Assertions.assertDoesNotThrow(jobDTO::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(jobDTO);
    Assertions.assertTrue(serJson.contains("\"finishedAt\""));

    JobDTO deserJobDTO = JsonUtils.objectMapper().readValue(serJson, JobDTO.class);
    Assertions.assertEquals(jobDTO, deserJobDTO);
    Assertions.assertEquals(finishedAt, deserJobDTO.finishedAt());
  }

  @Test
  public void testSerDeWithNullFinishedAt() throws JsonProcessingException {
    JobDTO jobDTO =
        new JobDTO(
            "job-456",
            "testTemplate",
            JobHandle.Status.QUEUED,
            AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build(),
            null);

    Assertions.assertDoesNotThrow(jobDTO::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(jobDTO);
    JobDTO deserJobDTO = JsonUtils.objectMapper().readValue(serJson, JobDTO.class);
    Assertions.assertEquals(jobDTO, deserJobDTO);
    Assertions.assertNull(deserJobDTO.finishedAt());
  }

  @Test
  public void testSerializeToString() throws JsonProcessingException {
    Instant createTime = Instant.parse("2024-01-01T00:00:00Z");
    Instant finishedAt = Instant.parse("2024-01-01T01:00:00Z");
    JobDTO jobDTO =
        new JobDTO(
            "job-789",
            "testTemplate",
            JobHandle.Status.FAILED,
            AuditDTO.builder().withCreator("test").withCreateTime(createTime).build(),
            finishedAt);

    String serJson = JsonUtils.objectMapper().writeValueAsString(jobDTO);

    Assertions.assertTrue(serJson.contains("\"jobId\":\"job-789\""));
    Assertions.assertTrue(serJson.contains("\"jobTemplateName\":\"testTemplate\""));
    Assertions.assertTrue(serJson.contains("\"status\":\"failed\""));
    Assertions.assertTrue(serJson.contains("\"finishedAt\":\"2024-01-01T01:00:00Z\""));
  }

  @Test
  public void testDeserializeFromString() throws JsonProcessingException {
    String json =
        "{"
            + "\"jobId\":\"job-999\","
            + "\"jobTemplateName\":\"testTemplate\","
            + "\"status\":\"succeeded\","
            + "\"audit\":{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"},"
            + "\"finishedAt\":\"2024-01-01T01:00:00Z\""
            + "}";

    JobDTO jobDTO = JsonUtils.objectMapper().readValue(json, JobDTO.class);

    Assertions.assertEquals("job-999", jobDTO.jobId());
    Assertions.assertEquals("testTemplate", jobDTO.jobTemplateName());
    Assertions.assertEquals(JobHandle.Status.SUCCEEDED, jobDTO.status());
    Assertions.assertEquals(Instant.parse("2024-01-01T01:00:00Z"), jobDTO.finishedAt());
  }

  @Test
  public void testDeserializeFromStringWithoutFinishedAt() throws JsonProcessingException {
    String json =
        "{"
            + "\"jobId\":\"job-1000\","
            + "\"jobTemplateName\":\"testTemplate\","
            + "\"status\":\"queued\","
            + "\"audit\":{\"creator\":\"test\",\"createTime\":\"2024-01-01T00:00:00Z\"}"
            + "}";

    JobDTO jobDTO = JsonUtils.objectMapper().readValue(json, JobDTO.class);

    Assertions.assertEquals("job-1000", jobDTO.jobId());
    Assertions.assertNull(jobDTO.finishedAt());
  }
}
