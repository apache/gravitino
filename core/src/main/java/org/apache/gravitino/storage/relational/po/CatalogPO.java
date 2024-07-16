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
    private final CatalogPO catalogPO;

    private Builder() {
      catalogPO = new CatalogPO();
    }

    public CatalogPO.Builder withCatalogId(Long catalogId) {
      catalogPO.catalogId = catalogId;
      return this;
    }

    public CatalogPO.Builder withCatalogName(String name) {
      catalogPO.catalogName = name;
      return this;
    }

    public CatalogPO.Builder withMetalakeId(Long metalakeId) {
      catalogPO.metalakeId = metalakeId;
      return this;
    }

    public CatalogPO.Builder withType(String type) {
      catalogPO.type = type;
      return this;
    }

    public CatalogPO.Builder withProvider(String provider) {
      catalogPO.provider = provider;
      return this;
    }

    public CatalogPO.Builder withCatalogComment(String comment) {
      catalogPO.catalogComment = comment;
      return this;
    }

    public CatalogPO.Builder withProperties(String properties) {
      catalogPO.properties = properties;
      return this;
    }

    public CatalogPO.Builder withAuditInfo(String auditInfo) {
      catalogPO.auditInfo = auditInfo;
      return this;
    }

    public CatalogPO.Builder withCurrentVersion(Long currentVersion) {
      catalogPO.currentVersion = currentVersion;
      return this;
    }

    public CatalogPO.Builder withLastVersion(Long lastVersion) {
      catalogPO.lastVersion = lastVersion;
      return this;
    }

    public CatalogPO.Builder withDeletedAt(Long deletedAt) {
      catalogPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(catalogPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(catalogPO.catalogName != null, "Catalog name is required");
      Preconditions.checkArgument(catalogPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(catalogPO.type != null, "Catalog type is required");
      Preconditions.checkArgument(catalogPO.provider != null, "Catalog provider is required");
      Preconditions.checkArgument(catalogPO.auditInfo != null, "Audit info is required");
      Preconditions.checkArgument(catalogPO.currentVersion != null, "Current version is required");
      Preconditions.checkArgument(catalogPO.lastVersion != null, "Last version is required");
      Preconditions.checkArgument(catalogPO.deletedAt != null, "Deleted at is required");
    }

    public CatalogPO build() {
      validate();
      return catalogPO;
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
