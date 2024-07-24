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

public class FilesetVersionPO {
  private Long id;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long filesetId;
  private Long version;
  private String filesetComment;
  private String properties;
  private String storageLocation;
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

  public Long getFilesetId() {
    return filesetId;
  }

  public Long getVersion() {
    return version;
  }

  public String getFilesetComment() {
    return filesetComment;
  }

  public String getProperties() {
    return properties;
  }

  public String getStorageLocation() {
    return storageLocation;
  }

  public Long getDeletedAt() {
    return deletedAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilesetVersionPO)) {
      return false;
    }
    FilesetVersionPO that = (FilesetVersionPO) o;
    return Objects.equal(getId(), that.getId())
        && Objects.equal(getMetalakeId(), that.getMetalakeId())
        && Objects.equal(getCatalogId(), that.getCatalogId())
        && Objects.equal(getSchemaId(), that.getSchemaId())
        && Objects.equal(getFilesetId(), that.getFilesetId())
        && Objects.equal(getVersion(), that.getVersion())
        && Objects.equal(getFilesetComment(), that.getFilesetComment())
        && Objects.equal(getProperties(), that.getProperties())
        && Objects.equal(getStorageLocation(), that.getStorageLocation())
        && Objects.equal(getDeletedAt(), that.getDeletedAt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getId(),
        getMetalakeId(),
        getCatalogId(),
        getSchemaId(),
        getFilesetId(),
        getVersion(),
        getFilesetComment(),
        getProperties(),
        getStorageLocation(),
        getDeletedAt());
  }

  public static class Builder {
    private final FilesetVersionPO filesetVersionPO;

    private Builder() {
      filesetVersionPO = new FilesetVersionPO();
    }

    public Builder withId(Long id) {
      filesetVersionPO.id = id;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      filesetVersionPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      filesetVersionPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      filesetVersionPO.schemaId = schemaId;
      return this;
    }

    public Builder withFilesetId(Long filesetId) {
      filesetVersionPO.filesetId = filesetId;
      return this;
    }

    public Builder withVersion(Long version) {
      filesetVersionPO.version = version;
      return this;
    }

    public Builder withFilesetComment(String filesetComment) {
      filesetVersionPO.filesetComment = filesetComment;
      return this;
    }

    public Builder withProperties(String properties) {
      filesetVersionPO.properties = properties;
      return this;
    }

    public Builder withStorageLocation(String storageLocation) {
      filesetVersionPO.storageLocation = storageLocation;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      filesetVersionPO.deletedAt = deletedAt;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(filesetVersionPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(filesetVersionPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(filesetVersionPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(filesetVersionPO.filesetId != null, "Fileset id is required");
      Preconditions.checkArgument(filesetVersionPO.version != null, "Fileset version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(filesetVersionPO.storageLocation), "Storage location is required");
      Preconditions.checkArgument(filesetVersionPO.deletedAt != null, "Deleted at is required");
    }

    public FilesetVersionPO build() {
      validate();
      return filesetVersionPO;
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
