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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.domain.DefinitionMember;
import org.apache.gravitino.domain.DefinitionMemberPage;

/** One page of direct definition members visible to a Domain administrator. */
public class DefinitionMemberListResponse extends BaseResponse {
  /** The fields exposed for each direct definition. */
  public static class Member implements DefinitionMember {
    @JsonProperty("type")
    private String type;

    @JsonProperty("name")
    private String name;

    /** Constructor for Jackson. */
    public Member() {}

    private Member(DefinitionMember member) {
      this.type = member.type().name();
      this.name = member.name();
    }

    @Override
    public Type type() {
      return Type.valueOf(type);
    }

    @Override
    public String name() {
      return name;
    }
  }

  @JsonProperty("members")
  private List<Member> members;

  @JsonProperty("hasMore")
  private boolean hasMore;

  /** Constructor for Jackson. */
  public DefinitionMemberListResponse() {}

  /** Builds the response from the authorized membership page. */
  public DefinitionMemberListResponse(DefinitionMemberPage page) {
    super(0);
    members = page.members().stream().map(Member::new).collect(Collectors.toList());
    hasMore = page.hasMore();
  }

  /** Returns the members in stable page order. */
  public List<Member> getMembers() {
    return members;
  }

  /** Returns whether another page follows. */
  public boolean hasMore() {
    return hasMore;
  }
}
