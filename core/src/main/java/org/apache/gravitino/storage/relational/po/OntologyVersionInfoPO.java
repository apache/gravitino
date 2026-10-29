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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * The persistent object for a complete ontology version snapshot.
 *
 * <p>The snapshot carries every mutable piece of content: the name, the comment, the Gravitino
 * properties and the definition sections. The ontology status is deliberately absent, because a
 * status change is a lifecycle flag flip on the identity row and must not allocate a version.
 *
 * <p>The {@link #requires()} property maps to the {@code requires_sql} column. The SQL column
 * cannot be called {@code requires} because that word is reserved in some dialects, while the
 * property keeps the name the Apache Ossie definition uses.
 */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class OntologyVersionInfoPO {

  private Long id;
  private Long metalakeId;
  private Long ontologyId;
  private Integer version;
  private String ontologyName;
  @Nullable private String ontologyComment;
  @Nullable private String aiContext;
  @Nullable private String requires;
  private String ontology;
  @Nullable private String prefixes;
  @Nullable private String properties;
  private String auditInfo;
  private Long deletedAt;

  /** Creates an empty persistent object for MyBatis. */
  public OntologyVersionInfoPO() {}

  @lombok.Builder(setterPrefix = "with")
  private OntologyVersionInfoPO(
      Long id,
      Long metalakeId,
      Long ontologyId,
      Integer version,
      String ontologyName,
      @Nullable String ontologyComment,
      @Nullable String aiContext,
      @Nullable String requires,
      String ontology,
      @Nullable String prefixes,
      @Nullable String properties,
      String auditInfo,
      Long deletedAt) {
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(ontologyId != null, "Ontology id is required");
    Preconditions.checkArgument(version != null, "Ontology version is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(ontologyName), "Ontology name cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(ontology), "Ontology concept graph cannot be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.id = id;
    this.metalakeId = metalakeId;
    this.ontologyId = ontologyId;
    this.version = version;
    this.ontologyName = ontologyName;
    this.ontologyComment = ontologyComment;
    this.aiContext = aiContext;
    this.requires = requires;
    this.ontology = ontology;
    this.prefixes = prefixes;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
  }
}
