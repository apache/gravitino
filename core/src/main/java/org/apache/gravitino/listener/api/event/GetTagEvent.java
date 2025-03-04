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
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event that is triggered upon successfully retrieving a tag. */
@DeveloperApi
public final class GetTagEvent extends TagEvent {
  private final TagInfo tagInfo;

  /**
   * Constructs an instance of {@code GetTagEvent}.
   *
   * @param user The username of the individual who initiated the tag retrieval.
   * @param metalake The metalake from which the tag was retrieved.
   * @param tagName The name of the tag being retrieved.
   * @param tagInfo The {@link TagInfo} object representing the retrieved tag.
   */
  public GetTagEvent(String user, String metalake, String tagName, TagInfo tagInfo) {
    super(user, NameIdentifierUtil.ofTag(metalake, tagName));
    this.tagInfo = tagInfo;
  }

  /**
   * Returns the {@link TagInfo} object representing the retrieved tag.
   *
   * @return The tag information.
   */
  public TagInfo tagInfo() {
    return tagInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_TAG;
  }
}
