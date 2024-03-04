/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class CatalogPO {
  private Long catalogId;
  private String catalogName;
  private Long metalakeId;
  private String type;
  private String provider;
  private String catalogComment;
  private String properties;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getCatalogId() {
    return catalogId;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public String getType() {
    return type;
  }

  public String getProvider() {
    return provider;
  }

  public String getCatalogComment() {
    return catalogComment;
  }

  public String getProperties() {
    return properties;
  }

  public String getAuditInfo() {
    return auditInfo;
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
    if (!(o instanceof CatalogPO)) {
      return false;
    }
    CatalogPO catalogPO = (CatalogPO) o;
    return Objects.equal(getCatalogId(), catalogPO.getCatalogId())
        && Objects.equal(getCatalogName(), catalogPO.getCatalogName())
        && Objects.equal(getMetalakeId(), catalogPO.getMetalakeId())
        && Objects.equal(getType(), catalogPO.getType())
        && Objects.equal(getProvider(), catalogPO.getProvider())
        && Objects.equal(getCatalogComment(), catalogPO.getCatalogComment())
        && Objects.equal(getProperties(), catalogPO.getProperties())
        && Objects.equal(getAuditInfo(), catalogPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), catalogPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), catalogPO.getLastVersion())
        && Objects.equal(getDeletedAt(), catalogPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getCatalogId(),
        getCatalogName(),
        getMetalakeId(),
        getType(),
        getProvider(),
        getCatalogComment(),
        getProperties(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final CatalogPO metalakePO;

    public Builder() {
      metalakePO = new CatalogPO();
    }

    public CatalogPO.Builder withCatalogId(Long catalogId) {
      metalakePO.catalogId = catalogId;
      return this;
    }

    public CatalogPO.Builder withCatalogName(String name) {
      metalakePO.catalogName = name;
      return this;
    }

    public CatalogPO.Builder withMetalakeId(Long metalakeId) {
      metalakePO.metalakeId = metalakeId;
      return this;
    }

    public CatalogPO.Builder withType(String type) {
      metalakePO.type = type;
      return this;
    }

    public CatalogPO.Builder withProvider(String provider) {
      metalakePO.provider = provider;
      return this;
    }

    public CatalogPO.Builder withCatalogComment(String comment) {
      metalakePO.catalogComment = comment;
      return this;
    }

    public CatalogPO.Builder withProperties(String properties) {
      metalakePO.properties = properties;
      return this;
    }

    public CatalogPO.Builder withAuditInfo(String auditInfo) {
      metalakePO.auditInfo = auditInfo;
      return this;
    }

    public CatalogPO.Builder withCurrentVersion(Long currentVersion) {
      metalakePO.currentVersion = currentVersion;
      return this;
    }

    public CatalogPO.Builder withLastVersion(Long lastVersion) {
      metalakePO.lastVersion = lastVersion;
      return this;
    }

    public CatalogPO.Builder withDeletedAt(Long deletedAt) {
      metalakePO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(metalakePO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(metalakePO.catalogName != null, "Catalog name is required");
      Preconditions.checkArgument(metalakePO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(metalakePO.type != null, "Catalog type is required");
      Preconditions.checkArgument(metalakePO.provider != null, "Catalog provider is required");
      Preconditions.checkArgument(metalakePO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(metalakePO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(metalakePO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(metalakePO.deletedAt != null, "Deleted at is required");
    }

    public CatalogPO build() {
      validate();
      return metalakePO;
    }
  }
}
