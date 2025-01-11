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
import org.apache.gravitino.tag.Tag;

/** Represents an event that is triggered upon the successful listing of tags. */
@DeveloperApi
public final class ListTagInfoEvent extends TagEvent {
  private final Tag[] tags;

  /**
   * Constructs an instance of {@code ListTagsEvent}.
   *
   * @param user The username of the individual who initiated the tag listing.
   * @param metalake The namespace from which tags were listed.
   * @param tags An array of {@link Tag} objects representing the tags.
   */
  public ListTagInfoEvent(String user, String metalake, Tag[] tags) {
    super(user, NameIdentifier.of(metalake));
    this.tags = tags;
  }

  /**
   * Provides the tags associated with this event.
   *
   * @return An array of {@link Tag} objects.
   */
  public Tag[] tags() {
    return tags;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.LIST_TAGS_INFO;
  }
}
