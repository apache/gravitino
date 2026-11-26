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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.apache.gravitino.domain.DefinitionMemberPage;
import org.apache.gravitino.dto.domain.DefinitionMemberDTO;

/** A page of direct definition members of a Domain. */
@Getter
public class DefinitionMemberListResponse extends BaseResponse {
  private final List<DefinitionMemberDTO> members;
  private final boolean hasMore;

  /**
   * Creates a successful response from a page of direct members.
   *
   * @param page the definition members and continuation flag
   */
  public DefinitionMemberListResponse(DefinitionMemberPage page) {
    this(
        page.members().stream()
            .map(member -> new DefinitionMemberDTO(member.type(), member.name()))
            .collect(ImmutableList.toImmutableList()),
        page.hasMore());
  }

  @JsonCreator
  private DefinitionMemberListResponse(
      @JsonProperty("members") List<DefinitionMemberDTO> members,
      @JsonProperty("hasMore") boolean hasMore) {
    super(0);
    this.members = ImmutableList.copyOf(members);
    this.hasMore = hasMore;
  }
}
