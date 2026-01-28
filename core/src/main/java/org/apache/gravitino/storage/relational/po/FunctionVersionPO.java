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

  public FunctionVersionPO() {}

  @lombok.Builder(setterPrefix = "with")
  private FunctionVersionPO(
      Long id,
      Long functionId,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Integer functionVersion,
      String functionComment,
      String definitions,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(functionId != null, "Function id is required");
    Preconditions.checkArgument(functionVersion != null, "Function version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(definitions), "Definitions cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.functionId = functionId;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.functionVersion = functionVersion;
    this.functionComment = functionComment;
    this.definitions = definitions;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }
}
