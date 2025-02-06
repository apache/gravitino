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

/** Represents an event triggered before the creation of a tag. */
@DeveloperApi
public class CreateTagPreEvent extends TagPreEvent {
  private final TagInfo tagInfo;

  /**
   * Constructs a new {@code CreateTagPreEvent} instance.
   *
   * @param user The user who initiated the tag creation operation.
   * @param metalake The metalake name where the tag resides.
   * @param tagInfo The information about the tag to be created.
   */
  public CreateTagPreEvent(String user, String metalake, TagInfo tagInfo) {
    super(user, NameIdentifierUtil.ofTag(metalake, tagInfo.name()));
    this.tagInfo = tagInfo;
  }

  /**
   * Returns the information about the tag.
   *
   * @return the tag information
   */
  public TagInfo tagInfo() {
    return tagInfo;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.CREATE_TAG;
  }
}
