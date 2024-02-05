/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.po;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;

public class CatalogPO {
  private Long id;
  private String catalogName;
  private Long metalakeId;
  private String type;
  private String provider;
  private String catalogComment;
  private String properties;
  private String auditInfo;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public void setMetalakeId(Long metalakeId) {
    this.metalakeId = metalakeId;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getProvider() {
    return provider;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public String getCatalogComment() {
    return catalogComment;
  }

  public void setCatalogComment(String catalogComment) {
    this.catalogComment = catalogComment;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CatalogPO)) {
      return false;
    }
    CatalogPO catalogPO = (CatalogPO) o;
    return Objects.equal(getId(), catalogPO.getId())
        && Objects.equal(getCatalogName(), catalogPO.getCatalogName())
        && Objects.equal(getMetalakeId(), catalogPO.getMetalakeId())
        && Objects.equal(getType(), catalogPO.getType())
        && Objects.equal(getProvider(), catalogPO.getProvider())
        && Objects.equal(getCatalogComment(), catalogPO.getCatalogComment())
        && Objects.equal(getProperties(), catalogPO.getProperties())
        && Objects.equal(getAuditInfo(), catalogPO.getAuditInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getId(),
        getCatalogName(),
        getMetalakeId(),
        getType(),
        getProvider(),
        getCatalogComment(),
        getProperties(),
        getAuditInfo());
  }

  public static class Builder {
    private final CatalogPO metalakePO;

    public Builder() {
      metalakePO = new CatalogPO();
    }

    public CatalogPO.Builder withId(Long id) {
      metalakePO.id = id;
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

    public CatalogPO.Builder withProperties(String properties) throws JsonProcessingException {
      metalakePO.properties = properties;
      return this;
    }

    public CatalogPO.Builder withAuditInfo(String auditInfo) throws JsonProcessingException {
      metalakePO.auditInfo = auditInfo;
      return this;
    }

    public CatalogPO build() {
      return metalakePO;
    }
  }
}
