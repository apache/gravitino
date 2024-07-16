/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class SchemaPO {
  private Long schemaId;
  private String schemaName;
  private Long metalakeId;
  private Long catalogId;
  private String schemaComment;
  private String properties;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public Long getSchemaId() {
    return schemaId;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public Long getMetalakeId() {
    return metalakeId;
  }

  public Long getCatalogId() {
    return catalogId;
  }

  public String getSchemaComment() {
    return schemaComment;
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
    if (!(o instanceof SchemaPO)) {
      return false;
    }
    SchemaPO schemaPO = (SchemaPO) o;
    return Objects.equal(getSchemaId(), schemaPO.getSchemaId())
        && Objects.equal(getSchemaName(), schemaPO.getSchemaName())
        && Objects.equal(getMetalakeId(), schemaPO.getMetalakeId())
        && Objects.equal(getCatalogId(), schemaPO.getCatalogId())
        && Objects.equal(getSchemaComment(), schemaPO.getSchemaComment())
        && Objects.equal(getProperties(), schemaPO.getProperties())
        && Objects.equal(getAuditInfo(), schemaPO.getAuditInfo())
        && Objects.equal(getCurrentVersion(), schemaPO.getCurrentVersion())
        && Objects.equal(getLastVersion(), schemaPO.getLastVersion())
        && Objects.equal(getDeletedAt(), schemaPO.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getSchemaId(),
        getSchemaName(),
        getMetalakeId(),
        getCatalogId(),
        getSchemaComment(),
        getProperties(),
        getAuditInfo(),
        getCurrentVersion(),
        getLastVersion(),
        getDeletedAt());
  }

  public static class Builder {
    private final SchemaPO schemaPO;

    private Builder() {
      schemaPO = new SchemaPO();
    }

    public Builder withSchemaId(Long schemaId) {
      schemaPO.schemaId = schemaId;
      return this;
    }

    public Builder withSchemaName(String schemaName) {
      schemaPO.schemaName = schemaName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      schemaPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      schemaPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaComment(String schemaComment) {
      schemaPO.schemaComment = schemaComment;
      return this;
    }

    public Builder withProperties(String properties) {
      schemaPO.properties = properties;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      schemaPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withCurrentVersion(Long currentVersion) {
      schemaPO.currentVersion = currentVersion;
      return this;
    }

    public Builder withLastVersion(Long lastVersion) {
      schemaPO.lastVersion = lastVersion;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      schemaPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(schemaPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(schemaPO.schemaName != null, "Schema name is required");
      Preconditions.checkArgument(schemaPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(schemaPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(schemaPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(schemaPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(schemaPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(schemaPO.deletedAt != null, "Deleted at is required");
    }

    public SchemaPO build() {
      validate();
      return schemaPO;
    }
  }
  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
