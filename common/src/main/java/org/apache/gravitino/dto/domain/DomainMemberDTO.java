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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.util.DTOConverters;

/** A recorded leaf member and all its relations to the queried domain. */
@Getter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DomainMemberDTO {

  @JsonProperty("object")
  private MetadataObjectDTO object;

  @JsonProperty("relations")
  private DomainRelationDTO[] relations;

  /**
   * Copies a member into its wire representation.
   *
   * @param member The member.
   * @return The member DTO.
   */
  public static DomainMemberDTO from(DomainMember member) {
    DomainMemberDTO dto = new DomainMemberDTO();
    dto.object = DTOConverters.toDTO(member.object());
    dto.relations =
        Arrays.stream(member.relations())
            .map(DomainRelationDTO::from)
            .toArray(DomainRelationDTO[]::new);
    return dto;
  }

  /**
   * @return All applicable relations.
   */
  public DomainRelationDTO[] getRelations() {
    return relations == null ? new DomainRelationDTO[0] : relations.clone();
  }
}
