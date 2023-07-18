/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.dto;

import com.datastrato.graviton.Audit;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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

  @Override
  public String creator() {
    return creator;
  }

  @Override
  public Instant createTime() {
    return createTime;
  }

  @Override
  public String lastModifier() {
    return lastModifier;
  }

  @Override
  public Instant lastModifiedTime() {
    return lastModifiedTime;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<S extends Builder> {
    protected String creator;
    protected Instant createTime;
    protected String lastModifier;
    protected Instant lastModifiedTime;

    public Builder() {}

    public S withCreator(String creator) {
      this.creator = creator;
      return (S) this;
    }

    public S withCreateTime(Instant createTime) {
      this.createTime = createTime;
      return (S) this;
    }

    public S withLastModifier(String lastModifier) {
      this.lastModifier = lastModifier;
      return (S) this;
    }

    public S withLastModifiedTime(Instant lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return (S) this;
    }

    public AuditDTO build() {
      return new AuditDTO(creator, createTime, lastModifier, lastModifiedTime);
    }
  }
}
