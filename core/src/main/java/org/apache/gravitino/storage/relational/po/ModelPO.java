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

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@EqualsAndHashCode
@Getter
public class ModelPO {

  private Long modelId;

  private String modelName;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private String modelComment;

  private Integer modelLatestVersion;

  private String modelProperties;

  private String auditInfo;

  private Long deletedAt;

  private ModelPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final ModelPO modelPO;

    private Builder() {
      modelPO = new ModelPO();
    }

    public Builder withModelId(Long modelId) {
      modelPO.modelId = modelId;
      return this;
    }

    public Builder withModelName(String modelName) {
      modelPO.modelName = modelName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      modelPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      modelPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      modelPO.schemaId = schemaId;
      return this;
    }

    public Builder withModelComment(String modelComment) {
      modelPO.modelComment = modelComment;
      return this;
    }

    public Builder withModelLatestVersion(Integer modelLatestVersion) {
      modelPO.modelLatestVersion = modelLatestVersion;
      return this;
    }

    public Builder withModelProperties(String modelProperties) {
      modelPO.modelProperties = modelProperties;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      modelPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      modelPO.deletedAt = deletedAt;
      return this;
    }

    public ModelPO build() {
      Preconditions.checkArgument(modelPO.modelId != null, "Model id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelPO.modelName), "Model name cannot be empty");
      Preconditions.checkArgument(modelPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(modelPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(modelPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(
          modelPO.modelLatestVersion != null, "Model latest version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelPO.auditInfo), "Audit info cannot be empty");
      Preconditions.checkArgument(modelPO.deletedAt != null, "Deleted at is required");
      return modelPO;
    }
  }
}
