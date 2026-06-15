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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;

/** Represents an event that is triggered upon the successful listing of tags. */
@DeveloperApi
public final class ListTagsEvent extends TagEvent implements ListEvent {

  private final int tagCount;

  /**
   * Constructs an instance of {@code ListTagsEvent}.
   *
   * @param user The username of the individual who initiated the tag listing.
   * @param metalake The metalake from which tags were listed.
   * @param tagCount The number of tags returned by the list operation.
   */
  public ListTagsEvent(String user, String metalake, int tagCount) {
    super(user, NameIdentifier.of(metalake));
    this.tagCount = tagCount;
  }

  /**
   * Constructs an instance of {@code ListTagsEvent} without a count.
   *
   * @param user The username of the individual who initiated the tag listing.
   * @param metalake The metalake from which tags were listed.
   * @deprecated Use {@link #ListTagsEvent(String, String, int)} instead.
   */
  @Deprecated
  @SuppressWarnings("InlineMeSuggester")
  public ListTagsEvent(String user, String metalake) {
    this(user, metalake, -1);
  }

  /** {@inheritDoc} */
  @Override
  public int resultCount() {
    return tagCount;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_TAG;
  }
}
