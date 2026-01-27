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
public class FunctionVersionPO {

  private Long id;

  private Long functionId;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private Integer functionVersion;

  private String functionComment;

  private String definitions;

  private String auditInfo;

  private Long deletedAt;

  private FunctionVersionPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final FunctionVersionPO functionVersionPO;

    private Builder() {
      functionVersionPO = new FunctionVersionPO();
    }

    public Builder withFunctionId(Long functionId) {
      functionVersionPO.functionId = functionId;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      functionVersionPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      functionVersionPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      functionVersionPO.schemaId = schemaId;
      return this;
    }

    public Builder withFunctionVersion(Integer functionVersion) {
      functionVersionPO.functionVersion = functionVersion;
      return this;
    }

    public Builder withFunctionComment(String functionComment) {
      functionVersionPO.functionComment = functionComment;
      return this;
    }

    public Builder withDefinitions(String definitions) {
      functionVersionPO.definitions = definitions;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      functionVersionPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      functionVersionPO.deletedAt = deletedAt;
      return this;
    }

    public FunctionVersionPO build() {
      Preconditions.checkArgument(functionVersionPO.functionId != null, "Function id is required");
      Preconditions.checkArgument(
          functionVersionPO.functionVersion != null, "Function version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(functionVersionPO.definitions), "Definitions cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(functionVersionPO.auditInfo), "Audit info cannot be empty");
      Preconditions.checkArgument(functionVersionPO.deletedAt != null, "Deleted at is required");

      return functionVersionPO;
    }
  }
}
