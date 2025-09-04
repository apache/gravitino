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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
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
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.job.JobTemplate;

/** Represents a Job Template Data Transfer Object (DTO). */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "jobType",
    visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = SparkJobTemplateDTO.class, name = "spark"),
  @JsonSubTypes.Type(value = ShellJobTemplateDTO.class, name = "shell"),
})
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@SuperBuilder(setterPrefix = "with")
@ToString
public abstract class JobTemplateDTO {

  @JsonDeserialize(using = JobTemplateDTO.JobTypeDeserializer.class)
  @JsonSerialize(using = JobTemplateDTO.JobTypeSerializer.class)
  @JsonProperty("jobType")
  private JobTemplate.JobType jobType;

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("executable")
  private String executable;

  @JsonProperty("arguments")
  private List<String> arguments;

  @JsonProperty("environments")
  private Map<String, String> environments;

  @JsonProperty("customFields")
  private Map<String, String> customFields;

  @JsonProperty("audit")
  private AuditDTO audit;

  /**
   * Default constructor for Jackson. This constructor is required for deserialization of the DTO.
   */
  protected JobTemplateDTO() {
    // Default constructor for Jackson
  }

  /** Validates the JobTemplateDTO. Ensures that required fields are not null or empty. */
  public void validate() {
    Preconditions.checkArgument(jobType != null, "\"jobType\" is required and cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(executable), "\"executable\" is required and cannot be empty");
  }

  /** Deserializer for JobTemplate.JobType. */
  public static class JobTypeDeserializer extends JsonDeserializer<JobTemplate.JobType> {

    @Override
    public JobTemplate.JobType deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      String value = p.getText();
      if (value != null) {
        try {
          return JobTemplate.JobType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw ctxt.weirdStringException(
              value, JobTemplate.JobType.class, "Invalid jobType value");
        }
      }

      throw ctxt.weirdStringException(
          null, JobTemplate.JobType.class, "jobType cannot be null or empty");
    }
  }

  /** Serializer for JobTemplate.JobType. */
  public static class JobTypeSerializer extends JsonSerializer<JobTemplate.JobType> {

    @Override
    public void serialize(
        JobTemplate.JobType value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      Preconditions.checkArgument(value != null, "\"jobType\" cannot be null");
      gen.writeString(value.name().toLowerCase());
    }
  }
}
