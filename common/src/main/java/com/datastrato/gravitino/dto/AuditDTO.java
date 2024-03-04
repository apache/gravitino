/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto;

import com.datastrato.gravitino.Audit;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Data transfer object representing audit information. */
@EqualsAndHashCode
@ToString
public class AuditDTO implements Audit {

  @JsonProperty("creator")
  private String creator;

  @JsonProperty("createTime")
  private Instant createTime;

  @JsonProperty("lastModifier")
  private String lastModifier;

  @JsonProperty("lastModifiedTime")
  private Instant lastModifiedTime;

  private AuditDTO() {}

  private AuditDTO(
      String creator, Instant createTime, String lastModifier, Instant lastModifiedTime) {
    this.creator = creator;
    this.createTime = createTime;
    this.lastModifier = lastModifier;
    this.lastModifiedTime = lastModifiedTime;
  }

  /** @return The creator of the audit. */
  @Override
  public String creator() {
    return creator;
  }

  /** @return The create time of the audit. */
  @Override
  public Instant createTime() {
    return createTime;
  }

  /** @return The last modifier of the audit. */
  @Override
  public String lastModifier() {
    return lastModifier;
  }

  /** @return The last modified time of the audit. */
  @Override
  public Instant lastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Creates a new Builder for constructing an Audit DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing an AuditDTO instance.
   *
   * @param <S> The type of the builder instance.
   */
  public static class Builder<S extends Builder> {

    /** The creator of the audit. */
    protected String creator;

    /** The create time for the audit. */
    protected Instant createTime;

    /** The last modifier of the audit. */
    protected String lastModifier;

    /** The last modified time for the audit. */
    protected Instant lastModifiedTime;

    /** * Default constructor for the builder. */
    public Builder() {}

    /**
     * Sets the creator for the audit.
     *
     * @param creator The creator of the audit.
     * @return The builder instance.
     */
    public S withCreator(String creator) {
      this.creator = creator;
      return (S) this;
    }

    /**
     * Sets the create time for the audit.
     *
     * @param createTime The create time of the audit.
     * @return The builder instance.
     */
    public S withCreateTime(Instant createTime) {
      this.createTime = createTime;
      return (S) this;
    }

    /**
     * Sets who last modified the audit.
     *
     * @param lastModifier The last modifier of the audit.
     * @return The builder instance.
     */
    public S withLastModifier(String lastModifier) {
      this.lastModifier = lastModifier;
      return (S) this;
    }

    /**
     * Sets the last modified time for the audit.
     *
     * @param lastModifiedTime The last modified time of the audit.
     * @return The builder instance.
     */
    public S withLastModifiedTime(Instant lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return (S) this;
    }

    /**
     * Builds an instance of AuditDTO using the builder's properties.
     *
     * @return An instance of AuditDTO.
     */
    public AuditDTO build() {
      return new AuditDTO(creator, createTime, lastModifier, lastModifiedTime);
    }
  }
}
