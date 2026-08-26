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

/** The persistent object for a complete Semantic Model version snapshot. */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class SemanticModelVersionInfoPO {

  private Long id;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long semanticModelId;
  private Integer version;
  private String semanticModelName;
  private String semanticModelComment;
  private String semanticModelDefinition;
  private String properties;
  private String auditInfo;
  private Long deletedAt;

  /** Creates an empty persistent object for MyBatis. */
  public SemanticModelVersionInfoPO() {}

  @lombok.Builder(setterPrefix = "with")
  private SemanticModelVersionInfoPO(
      Long id,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long semanticModelId,
      Integer version,
      String semanticModelName,
      String semanticModelComment,
      String semanticModelDefinition,
      String properties,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(semanticModelId != null, "Semantic Model id is required");
    Preconditions.checkArgument(version != null, "Semantic Model version is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(semanticModelName), "Semantic Model name cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(semanticModelDefinition),
        "Semantic Model definition cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.semanticModelId = semanticModelId;
    this.version = version;
    this.semanticModelName = semanticModelName;
    this.semanticModelComment = semanticModelComment;
    this.semanticModelDefinition = semanticModelDefinition;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }
}
