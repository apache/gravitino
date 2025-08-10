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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.io.IOException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.job.JobHandle;

/** Represents a Job Data Transfer Object (DTO). */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class JobDTO {

  @JsonProperty("jobId")
  private final String jobId;

  @JsonProperty("jobTemplateName")
  private final String jobTemplateName;

  @JsonProperty("status")
  @JsonSerialize(using = JobDTO.StatusSerializer.class)
  @JsonDeserialize(using = JobDTO.StatusDeserializer.class)
  private final JobHandle.Status status;

  @JsonProperty("audit")
  private final AuditDTO audit;

  /** Default constructor for Jackson deserialization. */
  private JobDTO() {
    this(null, null, null, null);
  }

  /**
   * Creates a new JobPO with the specified properties.
   *
   * @param jobId The unique identifier for the job.
   * @param jobTemplateName The name of the job template used for this job.
   * @param status The current status of the job.
   * @param audit The audit information associated with the job.
   */
  public JobDTO(String jobId, String jobTemplateName, JobHandle.Status status, AuditDTO audit) {
    this.jobId = jobId;
    this.jobTemplateName = jobTemplateName;
    this.status = status;
    this.audit = audit;
  }

  /**
   * Validates the JobDTO.
   *
   * @throws IllegalArgumentException if any of the required fields are invalid.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobId), "\"jobId\" is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobTemplateName),
        "\"jobTemplateName\" is required and cannot be empty");
    Preconditions.checkArgument(status != null, "\"status\" must not be null");
    Preconditions.checkArgument(audit != null, "\"audit\" must not be null");
  }

  /** Deserializer for Status */
  public static class StatusDeserializer extends JsonDeserializer<JobHandle.Status> {

    @Override
    public JobHandle.Status deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String value = p.getText();
      if (value != null) {
        try {
          return JobHandle.Status.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw ctxt.weirdStringException(value, JobHandle.Status.class, "Invalid status value");
        }
      }

      throw ctxt.weirdStringException(
          null, JobHandle.Status.class, "status cannot be null or empty");
    }
  }

  /** Serializer for Status. */
  public static class StatusSerializer extends JsonSerializer<JobHandle.Status> {

    @Override
    public void serialize(JobHandle.Status value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      Preconditions.checkArgument(value != null, "\"status\" cannot be null");
      gen.writeString(value.name().toLowerCase());
    }
  }
}
