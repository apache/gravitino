/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;

public class MetalakePO {
  private Long id;
  private String metalakeName;
  private String metalakeComment;
  private String properties;
  private String auditInfo;
  private String schemaVersion;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  public void setMetalakeName(String metalakeName) {
    this.metalakeName = metalakeName;
  }

  public String getMetalakeComment() {
    return metalakeComment;
  }

  public void setMetalakeComment(String metalakeComment) {
    this.metalakeComment = metalakeComment;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getAuditInfo() {
    return auditInfo;
  }

  public void setAuditInfo(String auditInfo) {
    this.auditInfo = auditInfo;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetalakePO)) {
      return false;
    }
    MetalakePO that = (MetalakePO) o;
    return Objects.equal(getId(), that.getId())
        && Objects.equal(getMetalakeName(), that.getMetalakeName())
        && Objects.equal(getMetalakeComment(), that.getMetalakeComment())
        && Objects.equal(getProperties(), that.getProperties())
        && Objects.equal(getAuditInfo(), that.getAuditInfo())
        && Objects.equal(getSchemaVersion(), that.getSchemaVersion());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getId(),
        getMetalakeName(),
        getMetalakeComment(),
        getProperties(),
        getAuditInfo(),
        getSchemaVersion());
  }

  public static class Builder {
    private final MetalakePO metalakePO;

    public Builder() {
      metalakePO = new MetalakePO();
    }

    public MetalakePO.Builder withId(Long id) {
      metalakePO.id = id;
      return this;
    }

    public MetalakePO.Builder withMetalakeName(String name) {
      metalakePO.metalakeName = name;
      return this;
    }

    public MetalakePO.Builder withMetalakeComment(String comment) {
      metalakePO.metalakeComment = comment;
      return this;
    }

    public MetalakePO.Builder withProperties(String properties) {
      metalakePO.properties = properties;
      return this;
    }

    public MetalakePO.Builder withAuditInfo(String auditInfo) {
      metalakePO.auditInfo = auditInfo;
      return this;
    }

    public MetalakePO.Builder withVersion(String version) {
      metalakePO.schemaVersion = version;
      return this;
    }

    public MetalakePO build() {
      return metalakePO;
    }
  }
}
