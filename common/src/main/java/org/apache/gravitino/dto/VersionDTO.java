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
package org.apache.gravitino.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a Version Data Transfer Object (DTO). */
@EqualsAndHashCode
@ToString
public class VersionDTO {

  @JsonProperty("version")
  private final String version;

  @JsonProperty("compileDate")
  private final String compileDate;

  @JsonProperty("gitCommit")
  private final String gitCommit;

  /** Default constructor for Jackson deserialization. */
  public VersionDTO() {
    this.version = "";
    this.compileDate = "";
    this.gitCommit = "";
  }

  /**
   * Creates a new instance of VersionDTO.
   *
   * @param version The version of the software.
   * @param compileDate The date the software was compiled.
   * @param gitCommit The git commit of the software.
   */
  public VersionDTO(String version, String compileDate, String gitCommit) {
    this.version = version;
    this.compileDate = compileDate;
    this.gitCommit = gitCommit;
  }

  /**
   * @return The version of the software.
   */
  public String version() {
    return version;
  }

  /**
   * @return The date the software was compiled.
   */
  public String compileDate() {
    return compileDate;
  }

  /**
   * @return The git commit of the software.
   */
  public String gitCommit() {
    return gitCommit;
  }
}
