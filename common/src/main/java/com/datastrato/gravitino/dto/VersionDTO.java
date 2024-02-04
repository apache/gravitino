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

  /** @return The version of the software. */
  public String version() {
    return version;
  }

  /** @return The date the software was compiled. */
  public String compileDate() {
    return compileDate;
  }

  /** @return The git commit of the software. */
  public String gitCommit() {
    return gitCommit;
  }
}
