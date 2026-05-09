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
public class ViewVersionInfoPO {

  private Long id;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private Long viewId;

  private Integer version;

  private String viewComment;

  private String columns;

  private String properties;

  private String defaultCatalog;

  private String defaultSchema;

  private String representations;

  private String auditInfo;

  private Long deletedAt;

  public ViewVersionInfoPO() {}

  @lombok.Builder(setterPrefix = "with")
  private ViewVersionInfoPO(
      Long id,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long viewId,
      Integer version,
      String viewComment,
      String columns,
      String properties,
      String defaultCatalog,
      String defaultSchema,
      String representations,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(viewId != null, "View id is required");
    Preconditions.checkArgument(version != null, "View version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(columns), "Columns cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(representations), "Representations cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.viewId = viewId;
    this.version = version;
    this.viewComment = viewComment;
    this.columns = columns;
    this.properties = properties;
    this.defaultCatalog = defaultCatalog;
    this.defaultSchema = defaultSchema;
    this.representations = representations;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }
}
