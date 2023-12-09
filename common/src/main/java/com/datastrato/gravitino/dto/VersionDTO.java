/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto;

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

  public VersionDTO() {
    this.version = "";
    this.compileDate = "";
    this.gitCommit = "";
  }

  public VersionDTO(String version, String compileDate, String gitCommit) {
    this.version = version;
    this.compileDate = compileDate;
    this.gitCommit = gitCommit;
  }

  public String version() {
    return version;
  }

  public String compileDate() {
    return compileDate;
  }

  public String gitCommit() {
    return gitCommit;
  }
}
