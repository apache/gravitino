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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.gravitino.job.JobTemplateChange;

/** DTO for updating a Job Template. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ShellTemplateUpdateDTO.class, name = "shell"),
  @JsonSubTypes.Type(value = SparkTemplateUpdateDTO.class, name = "spark")
})
@Getter
@EqualsAndHashCode
@SuperBuilder(setterPrefix = "with")
@ToString
public abstract class TemplateUpdateDTO {

  @Nullable
  @JsonProperty("newExecutable")
  private String newExecutable;

  @Nullable
  @JsonProperty("newArguments")
  private List<String> newArguments;

  @Nullable
  @JsonProperty("newEnvironments")
  private Map<String, String> newEnvironments;

  @Nullable
  @JsonProperty("newCustomFields")
  private Map<String, String> newCustomFields;

  /**
   * The default constructor for Jackson. This constructor is required for deserialization of the
   * DTO.
   */
  protected TemplateUpdateDTO() {
    // Default constructor for Jackson
  }

  /**
   * Converts this DTO to a TemplateUpdate object.
   *
   * @return the corresponding TemplateUpdate object
   */
  public abstract JobTemplateChange.TemplateUpdate toTemplateUpdate();
}
