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
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
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

  public FunctionPO() {}

  public static class FunctionPOBuilder {
    // Lombok will generate builder methods
  }

  @lombok.Builder(setterPrefix = "with")
  private FunctionPO(
      Long functionId,
      String functionName,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String functionType,
      Integer deterministic,
      String returnType,
      Integer functionLatestVersion,
      Integer functionCurrentVersion,
      String auditInfo,
      Long deletedAt,
      FunctionVersionPO functionVersionPO) {
    Preconditions.checkArgument(functionId != null, "Function id is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(functionName), "Function name cannot be empty");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(functionType), "Function type cannot be empty");
    Preconditions.checkArgument(
        functionLatestVersion != null, "Function latest version is required");
    Preconditions.checkArgument(
        functionCurrentVersion != null, "Function current version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.functionId = functionId;
    this.functionName = functionName;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.functionType = functionType;
    this.deterministic = deterministic;
    this.returnType = returnType;
    this.functionLatestVersion = functionLatestVersion;
    this.functionCurrentVersion = functionCurrentVersion;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
    this.functionVersionPO = functionVersionPO;
  }
}
