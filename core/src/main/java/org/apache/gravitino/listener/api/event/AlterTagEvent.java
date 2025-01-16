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
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.TagChange;

/** Represents an event triggered upon the successful alteration of a tag. */
@DeveloperApi
public final class AlterTagEvent extends TagEvent {
  private final TagInfo updatedTagInfo;
  private final TagChange[] tagChanges;

  /**
   * Constructs an instance of {@code AlterTagEvent}, encapsulating the key details about the
   * successful alteration of a tag.
   *
   * @param user The username of the individual responsible for initiating the tag alteration.
   * @param metalake The metalake from which the tag is being altered.
   * @param tagChanges An array of {@link TagChange} objects representing the specific changes
   *     applied to the tag during the alteration process.
   * @param updatedTagInfo The post-alteration state of the tag.
   */
  public AlterTagEvent(
      String user, String metalake, TagChange[] tagChanges, TagInfo updatedTagInfo) {
    super(user, NameIdentifier.of(metalake));
    this.tagChanges = tagChanges;
    this.updatedTagInfo = updatedTagInfo;
  }

  /**
   * Retrieves the final state of the tag as it was returned to the user after successful
   * alteration.
   *
   * @return A {@link TagInfo} instance encapsulating the comprehensive details of the newly altered
   *     tag.
   */
  public TagInfo updatedTagInfo() {
    return updatedTagInfo;
  }

  /**
   * Retrieves the specific changes that were made to the tag during the alteration process.
   *
   * @return An array of {@link TagChange} objects detailing each modification applied to the tag.
   */
  public TagChange[] tagChanges() {
    return tagChanges;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ALTER_TAG;
  }
}
