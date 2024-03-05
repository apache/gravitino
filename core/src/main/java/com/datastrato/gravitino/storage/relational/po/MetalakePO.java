/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class MetalakePO {
  private Long metalakeId;
  private String metalakeName;
  private String metalakeComment;
  private String properties;
  private String auditInfo;
  private String schemaVersion;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getMetalakeId() {
    return metalakeId;
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  public String getMetalakeComment() {
    return metalakeComment;
  }

  public String getProperties() {
    return properties;
  }

  public String getAuditInfo() {
    return auditInfo;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public Long getCurrentVersion() {
    return currentVersion;
  }

  public Long getLastVersion() {
    return lastVersion;
  }

  public Long getDeletedAt() {
    return deletedAt;
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
    return Objects.equal(getMetalakeId(), that.getMetalakeId())
        && Objects.equal(getMetalakeName(), that.getMetalakeName())
        && Objects.equal(getMetalakeComment(), that.getMetalakeComment())
        && Objects.equal(getProperties(), that.getProperties())
        && Objects.equal(getAuditInfo(), that.getAuditInfo())
        && Objects.equal(getSchemaVersion(), that.getSchemaVersion())
        && Objects.equal(getCurrentVersion(), that.getCurrentVersion())
        && Objects.equal(getLastVersion(), that.getLastVersion())
        && Objects.equal(getDeletedAt(), that.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getMetalakeId(),
        getMetalakeName(),
        getMetalakeComment(),
        getProperties(),
        getAuditInfo(),
        getSchemaVersion(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final MetalakePO metalakePO;

    public Builder() {
      metalakePO = new MetalakePO();
    }

    public MetalakePO.Builder withMetalakeId(Long id) {
      metalakePO.metalakeId = id;
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

    public MetalakePO.Builder withSchemaVersion(String version) {
      metalakePO.schemaVersion = version;
      return this;
    }

    public MetalakePO.Builder withCurrentVersion(Long currentVersion) {
      metalakePO.currentVersion = currentVersion;
      return this;
    }

    public MetalakePO.Builder withLastVersion(Long lastVersion) {
      metalakePO.lastVersion = lastVersion;
      return this;
    }

    public MetalakePO.Builder withDeletedAt(Long deletedAt) {
      metalakePO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(metalakePO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(metalakePO.metalakeName != null, "Metalake name is required");
      Preconditions.checkArgument(metalakePO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(metalakePO.schemaVersion != null, "Schema version is required");
      Preconditions.checkArgument(metalakePO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(metalakePO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(metalakePO.deletedAt != null, "Deleted at is required");
    }

    public MetalakePO build() {
      validate();
      return metalakePO;
    }
  }
}
