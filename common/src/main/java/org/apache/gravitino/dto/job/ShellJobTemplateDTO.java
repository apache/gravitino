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
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;

/** Represents a Shell Job Template Data Transfer Object (DTO). */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(setterPrefix = "with")
@ToString(callSuper = true)
public class ShellJobTemplateDTO extends JobTemplateDTO {

  @JsonProperty("scripts")
  private List<String> scripts;

  /** Creates a new ShellJobTemplateDTO with the specified properties. */
  private ShellJobTemplateDTO() {
    // Default constructor for Jackson
    super();
  }
}
