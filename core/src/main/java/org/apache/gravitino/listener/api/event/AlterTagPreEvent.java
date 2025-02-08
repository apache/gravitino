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
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered before altering a tag. */
@DeveloperApi
public class AlterTagPreEvent extends TagPreEvent {

  private final TagChange[] changes;

  /**
   * Constructs a new AlterTagPreEvent instance.
   *
   * @param user The user responsible for the operation.
   * @param metalake The namespace of the tag.
   * @param name The name of the tag being altered.
   * @param changes The changes being applied to the tag.
   */
  public AlterTagPreEvent(String user, String metalake, String name, TagChange[] changes) {
    super(user, NameIdentifierUtil.ofTag(metalake, name));
    this.changes = changes;
  }

  /**
   * Returns the changes being applied to the tag.
   *
   * @return An array of {@link TagChange}.
   */
  public TagChange[] changes() {
    return changes;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return The operation type {@link OperationType#ALTER_TAG}.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_TAG;
  }
}
