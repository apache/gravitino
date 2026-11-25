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

package org.apache.gravitino.domain;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.annotation.Evolving;

/** A bounded page of direct definitions listed for a Domain administrator. */
@Evolving
public final class DefinitionMemberPage {
  private final List<DefinitionMember> members;
  private final boolean hasMore;

  /**
   * Creates a page of direct definition members.
   *
   * @param members the members in this window
   * @param hasMore whether another direct member follows
   */
  public DefinitionMemberPage(List<DefinitionMember> members, boolean hasMore) {
    this.members = ImmutableList.copyOf(members);
    this.hasMore = hasMore;
  }

  /**
   * @return the direct definition members on this page
   */
  public List<DefinitionMember> members() {
    return members;
  }

  /**
   * @return whether more direct definition members follow this page
   */
  public boolean hasMore() {
    return hasMore;
  }
}
