/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class TablePO {
  private Long tableId;
  private String tableName;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getTableId() {
    return tableId;
  }

  public String getTableName() {
    return tableName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public Long getCatalogId() {
    return catalogId;
  }

  public Long getSchemaId() {
    return schemaId;
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
    if (!(o instanceof TablePO)) {
      return false;
    }
    TablePO tablePO = (TablePO) o;
    return Objects.equal(getTableId(), tablePO.getTableId())
        && Objects.equal(getTableName(), tablePO.getTableName())
        && Objects.equal(getMetalakeId(), tablePO.getMetalakeId())
        && Objects.equal(getCatalogId(), tablePO.getCatalogId())
        && Objects.equal(getSchemaId(), tablePO.getSchemaId())
        && Objects.equal(getAuditInfo(), tablePO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), tablePO.getCurrentVersion())
        && Objects.equal(getLastVersion(), tablePO.getLastVersion())
        && Objects.equal(getDeletedAt(), tablePO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getTableId(),
        getTableName(),
        getMetalakeId(),
        getCatalogId(),
        getSchemaId(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final TablePO tablePO;

    public Builder() {
      tablePO = new TablePO();
    }

    public Builder withTableId(Long tableId) {
      tablePO.tableId = tableId;
      return this;
    }

    public Builder withTableName(String tableName) {
      tablePO.tableName = tableName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      tablePO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      tablePO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      tablePO.schemaId = schemaId;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      tablePO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      tablePO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      tablePO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      tablePO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(tablePO.tableId != null, "Table id is required");
      Preconditions.checkArgument(tablePO.tableName != null, "Table name is required");
      Preconditions.checkArgument(tablePO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(tablePO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(tablePO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(tablePO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(tablePO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(tablePO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(tablePO.deletedAt != null, "Deleted at is required");
    }

    public TablePO build() {
      validate();
      return tablePO;
    }
  }
}
