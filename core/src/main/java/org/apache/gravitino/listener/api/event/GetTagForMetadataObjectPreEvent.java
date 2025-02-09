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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.MetadataObjectUtil;

/** Represents an event triggered before retrieving a tag for a specific metadata object. */
@DeveloperApi
public class GetTagForMetadataObjectPreEvent extends TagPreEvent {
  private final String tagName;

  /**
   * Constructs the pre-event with user, metalake, metadata object, and tag name.
   *
   * @param user The user initiating the operation.
   * @param metalake The metalake environment name.
   * @param metadataObject The metadata object associated with the tag.
   * @param name The name of the tag being retrieved.
   */
  public GetTagForMetadataObjectPreEvent(
      String user, String metalake, MetadataObject metadataObject, String name) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject));
    this.tagName = name;
  }

  /**
   * Returns the name of the tag being retrieved.
   *
   * @return The tag name.
   */
  public String tagName() {
    return tagName;
  }

  /**
   * Returns the operation type for this event.
   *
   * @return The operation type, which is GET_TAG_FOR_METADATA_OBJECT.
   */
  @Override
  public OperationType operationType() {
    return OperationType.GET_TAG_FOR_METADATA_OBJECT;
  }
}
