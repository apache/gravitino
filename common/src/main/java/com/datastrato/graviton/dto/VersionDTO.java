/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a Version Data Transfer Object (DTO). */
@EqualsAndHashCode
@ToString
public class VersionDTO {

  @JsonProperty("version")
  private String version;

  @JsonProperty("compileDate")
  private String compileDate;

  @JsonProperty("gitCommit")
  private String gitCommit;

  protected VersionDTO() {}

  protected VersionDTO(String version, String compileDate, String gitCommit) {
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

  /**
   * A builder class for constructing instances of VersionDTO.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {

    protected String version;
    protected String compileDate;
    protected String gitCommit;

    public Builder() {}

    /**
     * Sets the version of the Version DTO.
     *
     * @param version The version of the Version DTO.
     * @return The builder instance.
     */
    public S withVersion(String version) {
      this.version = version;
      return (S) this;
    }

    /**
     * Sets the compile date of the Version DTO.
     *
     * @param compileDate The comment of the Version DTO.
     * @return The builder instance.
     */
    public S withComment(String compileDate) {
      this.compileDate = compileDate;
      return (S) this;
    }

    /**
     * Sets the Git commit id of the Version DTO.
     *
     * @param gitCommit The Git commit id of the Version DTO.
     * @return The builder instance.
     */
    public S withGitCommit(String gitCommit) {
      this.gitCommit = gitCommit;
      return (S) this;
    }

    /**
     * Builds an instance of VersionDTO using the builder's properties.
     *
     * @return An instance of VersionDTO.
     * @throws IllegalArgumentException If the name or compileDate or gitCommit are not set.
     */
    public VersionDTO build() {
      Preconditions.checkArgument(
          version != null && !version.isEmpty(), "version cannot be null or empty");
      Preconditions.checkArgument(compileDate != null, "compile date cannot be null");
      Preconditions.checkArgument(gitCommit != null, "Git commit id cannot be null");
      return new VersionDTO(version, compileDate, gitCommit);
    }
  }
}
