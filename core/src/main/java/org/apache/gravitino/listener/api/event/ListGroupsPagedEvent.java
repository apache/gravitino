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
package org.apache.gravitino.listener.api.event;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after successfully listing groups with pagination. */
@DeveloperApi
public class ListGroupsPagedEvent extends GroupEvent implements ListEvent {

  private final int offset;
  private final int limit;
  private final int pageSize;
  private final long totalCount;

  /**
   * Creates a new {@link ListGroupsPagedEvent}.
   *
   * @param initiator the user who initiated the request.
   * @param metalake the metalake name.
   * @param offset the number of groups skipped.
   * @param limit the requested page size limit.
   * @param pageSize the number of groups returned in this page.
   * @param totalCount the total number of groups in the metalake.
   */
  public ListGroupsPagedEvent(
      String initiator, String metalake, int offset, int limit, int pageSize, long totalCount) {
    super(initiator, NameIdentifierUtil.ofMetalake(metalake));
    this.offset = offset;
    this.limit = limit;
    this.pageSize = pageSize;
    this.totalCount = totalCount;
  }

  /**
   * Returns the pagination offset.
   *
   * @return the offset.
   */
  public int offset() {
    return offset;
  }

  /**
   * Returns the pagination limit.
   *
   * @return the limit.
   */
  public int limit() {
    return limit;
  }

  /**
   * Returns the total number of groups in the metalake.
   *
   * @return the total count.
   */
  public long totalCount() {
    return totalCount;
  }

  @Override
  public int resultCount() {
    return pageSize;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_GROUPS_PAGED;
  }
}
