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
import org.apache.commons.lang3.StringUtils;

public class LanceTableVersionPO {
  private Long id;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long tableId;
  private Long version;
  private String manifestPath;
  private Long manifestSize;
  private String eTag;
  private String namingScheme;
  private String metadata;
  private String context;
  private Long createdAt;
  private String createdBy;
  private String requestId;
  private Long deletedAt;

  public Long getId() {
    return id;
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

  public Long getTableId() {
    return tableId;
  }

  public Long getVersion() {
    return version;
  }

  public String getManifestPath() {
    return manifestPath;
  }

  public Long getManifestSize() {
    return manifestSize;
  }

  public String getETag() {
    return eTag;
  }

  public String getNamingScheme() {
    return namingScheme;
  }

  public String getMetadata() {
    return metadata;
  }

  public String getContext() {
    return context;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public String getRequestId() {
    return requestId;
  }

  public Long getDeletedAt() {
    return deletedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LanceTableVersionPO)) {
      return false;
    }
    LanceTableVersionPO that = (LanceTableVersionPO) o;
    return Objects.equal(getId(), that.getId())
        && Objects.equal(getMetalakeId(), that.getMetalakeId())
        && Objects.equal(getCatalogId(), that.getCatalogId())
        && Objects.equal(getSchemaId(), that.getSchemaId())
        && Objects.equal(getTableId(), that.getTableId())
        && Objects.equal(getVersion(), that.getVersion())
        && Objects.equal(getManifestPath(), that.getManifestPath())
        && Objects.equal(getManifestSize(), that.getManifestSize())
        && Objects.equal(getETag(), that.getETag())
        && Objects.equal(getNamingScheme(), that.getNamingScheme())
        && Objects.equal(getMetadata(), that.getMetadata())
        && Objects.equal(getContext(), that.getContext())
        && Objects.equal(getCreatedAt(), that.getCreatedAt())
        && Objects.equal(getCreatedBy(), that.getCreatedBy())
        && Objects.equal(getRequestId(), that.getRequestId())
        && Objects.equal(getDeletedAt(), that.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getId(),
        getMetalakeId(),
        getCatalogId(),
        getSchemaId(),
        getTableId(),
        getVersion(),
        getManifestPath(),
        getManifestSize(),
        getETag(),
        getNamingScheme(),
        getMetadata(),
        getContext(),
        getCreatedAt(),
        getCreatedBy(),
        getRequestId(),
        getDeletedAt());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final LanceTableVersionPO po;

    private Builder() {
      this.po = new LanceTableVersionPO();
    }

    public Builder withId(Long id) {
      po.id = id;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      po.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      po.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      po.schemaId = schemaId;
      return this;
    }

    public Builder withTableId(Long tableId) {
      po.tableId = tableId;
      return this;
    }

    public Builder withVersion(Long version) {
      po.version = version;
      return this;
    }

    public Builder withManifestPath(String manifestPath) {
      po.manifestPath = manifestPath;
      return this;
    }

    public Builder withManifestSize(Long manifestSize) {
      po.manifestSize = manifestSize;
      return this;
    }

    public Builder withETag(String eTag) {
      po.eTag = eTag;
      return this;
    }

    public Builder withNamingScheme(String namingScheme) {
      po.namingScheme = namingScheme;
      return this;
    }

    public Builder withMetadata(String metadata) {
      po.metadata = metadata;
      return this;
    }

    public Builder withContext(String context) {
      po.context = context;
      return this;
    }

    public Builder withCreatedAt(Long createdAt) {
      po.createdAt = createdAt;
      return this;
    }

    public Builder withCreatedBy(String createdBy) {
      po.createdBy = createdBy;
      return this;
    }

    public Builder withRequestId(String requestId) {
      po.requestId = requestId;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      po.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(po.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(po.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(po.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(po.tableId != null, "Table id is required");
      Preconditions.checkArgument(po.version != null, "Version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(po.manifestPath), "Manifest path is required");
      Preconditions.checkArgument(po.createdAt != null, "Created at is required");
      Preconditions.checkArgument(po.deletedAt != null, "Deleted at is required");
    }

    public LanceTableVersionPO build() {
      validate();
      return po;
    }
  }
}
