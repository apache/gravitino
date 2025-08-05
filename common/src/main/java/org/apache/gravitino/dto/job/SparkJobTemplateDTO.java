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
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

/** Represents a Spark Job Template Data Transfer Object (DTO). */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(setterPrefix = "with")
@ToString(callSuper = true)
public class SparkJobTemplateDTO extends JobTemplateDTO {

  @JsonProperty("className")
  private String className;

  @JsonProperty("jars")
  private List<String> jars;

  @JsonProperty("files")
  private List<String> files;

  @JsonProperty("archives")
  private List<String> archives;

  @JsonProperty("configs")
  private Map<String, String> configs;

  /** Creates a new SparkJobTemplateDTO with the specified properties. */
  private SparkJobTemplateDTO() {
    // Default constructor for Jackson
    super();
  }

  @Override
  public void validate() {
    super.validate();

    Preconditions.checkArgument(
        StringUtils.isNotBlank(className), "\"className\" is required and cannot be empty");
  }
}
