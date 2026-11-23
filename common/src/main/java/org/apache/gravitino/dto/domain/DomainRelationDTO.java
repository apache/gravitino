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
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.util.DTOConverters;

/** Describes one relation between a metadata object and a domain. */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DomainRelationDTO {

  @JsonProperty("relation")
  private DomainRelation.Type relation;

  @JsonProperty("source")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  private MetadataObjectDTO source;

  /**
   * Copies a relation into its wire representation.
   *
   * @param relation The relation.
   * @return The relation DTO.
   */
  public static DomainRelationDTO from(DomainRelation relation) {
    DomainRelationDTO dto = new DomainRelationDTO();
    dto.relation = relation.type();
    dto.source = relation.source().map(DTOConverters::toDTO).orElse(null);
    return dto;
  }
}
