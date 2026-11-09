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
package org.apache.gravitino.dto.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.tag.TagValue;

/**
 * A domain snapshot; relations are present only in a metadata object's domain lookup. Jackson uses
 * the private no-argument constructor to deserialize a snapshot.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DomainDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  @Nullable
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("status")
  private Domain.Status status;

  @JsonProperty("parent")
  @Nullable
  private String parent;

  @JsonProperty("backingTag")
  @Nullable
  private DomainBackingTagDTO backingTag;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("relations")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private DomainRelationDTO[] relations = new DomainRelationDTO[0];

  /**
   * Creates a domain response snapshot.
   *
   * @param name The domain name.
   * @param comment The optional comment.
   * @param properties The properties.
   * @param status The status.
   * @param parent The optional parent name.
   * @param backingTag The optional backing tag.
   * @param audit The audit information.
   * @param relations The object's relations to this domain.
   */
  public DomainDTO(
      String name,
      @Nullable String comment,
      Map<String, String> properties,
      Domain.Status status,
      @Nullable String parent,
      @Nullable TagValue backingTag,
      AuditDTO audit,
      DomainRelation[] relations) {
    this.name = name;
    this.comment = comment;
    this.properties = ImmutableMap.copyOf(properties);
    this.status = status;
    this.parent = parent;
    this.backingTag =
        backingTag == null
            ? null
            : new DomainBackingTagDTO(backingTag.name(), backingTag.value().orElse(null));
    this.audit = audit;
    this.relations =
        Arrays.stream(relations).map(DomainRelationDTO::from).toArray(DomainRelationDTO[]::new);
  }

  /**
   * @return All applicable relations.
   */
  public DomainRelationDTO[] getRelations() {
    return relations == null ? new DomainRelationDTO[0] : relations.clone();
  }
}
