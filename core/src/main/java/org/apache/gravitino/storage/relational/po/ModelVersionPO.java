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
public class ModelVersionPO {

  private Long modelId;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private Integer modelVersion;

  private String modelVersionComment;

  private String modelVersionProperties;

  private String modelVersionUriName;

  private String modelVersionUri;

  private String auditInfo;

  private Long deletedAt;

  private ModelVersionPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final ModelVersionPO modelVersionPO;

    private Builder() {
      modelVersionPO = new ModelVersionPO();
    }

    public Builder withModelId(Long modelId) {
      modelVersionPO.modelId = modelId;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      modelVersionPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      modelVersionPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      modelVersionPO.schemaId = schemaId;
      return this;
    }

    public Builder withModelVersion(Integer modelVersion) {
      modelVersionPO.modelVersion = modelVersion;
      return this;
    }

    public Builder withModelVersionComment(String modelVersionComment) {
      modelVersionPO.modelVersionComment = modelVersionComment;
      return this;
    }

    public Builder withModelVersionProperties(String modelVersionProperties) {
      modelVersionPO.modelVersionProperties = modelVersionProperties;
      return this;
    }

    public Builder withModelVersionUriName(String modelVersionUriName) {
      modelVersionPO.modelVersionUriName = modelVersionUriName;
      return this;
    }

    public Builder withModelVersionUri(String modelVersionUri) {
      modelVersionPO.modelVersionUri = modelVersionUri;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      modelVersionPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      modelVersionPO.deletedAt = deletedAt;
      return this;
    }

    public ModelVersionPO build() {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelVersionPO.modelVersionUriName),
          "Model version uri name cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelVersionPO.modelVersionUri),
          "Model version uri cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(modelVersionPO.auditInfo), "Audit info cannot be empty");
      Preconditions.checkArgument(modelVersionPO.deletedAt != null, "Deleted at is required");

      return modelVersionPO;
    }
  }
}
