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

/**
 * Represents an event triggered when an attempt to alter a tag in the database fails due to an
 * exception.
 */
@DeveloperApi
public class AlterTagFailureEvent extends TagFailureEvent {
  private final TagChange[] changes;

  /**
   * Constructs a new AlterTagFailureEvent.
   *
   * @param user the user who attempted to alter the tag
   * @param metalake the metalake identifier
   * @param name the name of the tag
   * @param changes the changes attempted to be made to the tag
   * @param exception the exception that caused the failure
   */
  public AlterTagFailureEvent(
      String user, String metalake, String name, TagChange[] changes, Exception exception) {
    super(user, NameIdentifierUtil.ofTag(metalake, name), exception);
    this.changes = changes;
  }

  /**
   * Returns the changes attempted to be made to the tag.
   *
   * @return the changes attempted to be made to the tag
   */
  public TagChange[] changes() {
    return changes;
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
