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
public class FunctionPO {

  private Long functionId;

  private String functionName;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private String functionType;

  private Integer deterministic;

  private String returnType;

  private Integer functionLatestVersion;

  private Integer functionCurrentVersion;

  private String auditInfo;

  private Long deletedAt;

  private FunctionVersionPO functionVersionPO;

  private FunctionPO() {}

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final FunctionPO functionPO;

    private Builder() {
      functionPO = new FunctionPO();
    }

    public Builder withFunctionId(Long functionId) {
      functionPO.functionId = functionId;
      return this;
    }

    public Builder withFunctionName(String functionName) {
      functionPO.functionName = functionName;
      return this;
    }

    public Builder withMetalakeId(Long metalakeId) {
      functionPO.metalakeId = metalakeId;
      return this;
    }

    public Builder withCatalogId(Long catalogId) {
      functionPO.catalogId = catalogId;
      return this;
    }

    public Builder withSchemaId(Long schemaId) {
      functionPO.schemaId = schemaId;
      return this;
    }

    public Builder withFunctionType(String functionType) {
      functionPO.functionType = functionType;
      return this;
    }

    public Builder withDeterministic(Integer deterministic) {
      functionPO.deterministic = deterministic;
      return this;
    }

    public Builder withReturnType(String returnType) {
      functionPO.returnType = returnType;
      return this;
    }

    public Builder withFunctionLatestVersion(Integer functionLatestVersion) {
      functionPO.functionLatestVersion = functionLatestVersion;
      return this;
    }

    public Builder withFunctionCurrentVersion(Integer functionCurrentVersion) {
      functionPO.functionCurrentVersion = functionCurrentVersion;
      return this;
    }

    public Builder withAuditInfo(String auditInfo) {
      functionPO.auditInfo = auditInfo;
      return this;
    }

    public Builder withDeletedAt(Long deletedAt) {
      functionPO.deletedAt = deletedAt;
      return this;
    }

    public Builder withFunctionVersionPO(FunctionVersionPO functionVersionPO) {
      functionPO.functionVersionPO = functionVersionPO;
      return this;
    }

    public Long getMetalakeId() {
      Preconditions.checkState(functionPO.metalakeId != null, "Metalake id is null");
      return functionPO.metalakeId;
    }

    public Long getCatalogId() {
      Preconditions.checkState(functionPO.catalogId != null, "Catalog id is null");
      return functionPO.catalogId;
    }

    public Long getSchemaId() {
      Preconditions.checkState(functionPO.schemaId != null, "Schema id is null");
      return functionPO.schemaId;
    }

    public Integer getFunctionCurrentVersion() {
      Preconditions.checkState(
          functionPO.functionCurrentVersion != null, "Function current version is null");
      return functionPO.functionCurrentVersion;
    }

    public FunctionPO build() {
      Preconditions.checkArgument(functionPO.functionId != null, "Function id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(functionPO.functionName), "Function name cannot be empty");
      Preconditions.checkArgument(functionPO.metalakeId != null, "Metalake id is required");
      Preconditions.checkArgument(functionPO.catalogId != null, "Catalog id is required");
      Preconditions.checkArgument(functionPO.schemaId != null, "Schema id is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(functionPO.functionType), "Function type cannot be empty");
      Preconditions.checkArgument(
          functionPO.functionLatestVersion != null, "Function latest version is required");
      Preconditions.checkArgument(
          functionPO.functionCurrentVersion != null, "Function current version is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(functionPO.auditInfo), "Audit info cannot be empty");
      Preconditions.checkArgument(functionPO.deletedAt != null, "Deleted at is required");
      return functionPO;
    }
  }
}
