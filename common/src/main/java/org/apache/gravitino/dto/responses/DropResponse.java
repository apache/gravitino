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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a drop operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class DropResponse extends BaseResponse {

  @JsonProperty("dropped")
  private final Boolean dropped;

  /**
   * This field is added to be compatible with DeleteResponse. This field will only be set for
   * deleteRole operation. For other drop operations, it will be null. This field will be leveraged
   * by the old client, for new client it will never be leveraged.
   */
  @JsonProperty("deleted")
  private final Boolean deleted;

  /**
   * Constructor for DropResponse.
   *
   * @param dropped Whether the drop operation was successful.
   */
  public DropResponse(Boolean dropped) {
    super(0);
    this.dropped = dropped;
    this.deleted = null;
  }

  /**
   * Constructor for DropResponse with both dropped and deleted fields.
   *
   * @param dropped Whether the drop operation was successful.
   * @param deleted Whether the delete operation was successful (used for backward compatibility).
   */
  public DropResponse(Boolean dropped, Boolean deleted) {
    super(0);
    this.dropped = dropped;
    this.deleted = deleted;
  }

  /** Default constructor for DropResponse (used by Jackson deserializer). */
  public DropResponse() {
    super();
    this.dropped = null;
    this.deleted = null;
  }

  /**
   * Returns whether the drop operation was successful.
   *
   * @return True if the drop operation was successful, otherwise false.
   */
  public Boolean dropped() {
    // If the dropped field is null, it means we use the new client to handle the old server
    // `DeleteResponse` response, so we should return the deleted field.
    return dropped != null ? dropped : deleted;
  }

  /**
   * Returns whether the delete operation was successful. This method will only be called in test.
   *
   * @return True if the delete operation was successful, otherwise false.
   */
  @VisibleForTesting
  public Boolean deleted() {
    return deleted;
  }

  @Override
  public void validate() {
    super.validate();

    // Ensure that at least one of dropped or deleted is set
    Preconditions.checkArgument(
        dropped != null || deleted != null,
        "Either 'dropped' or 'deleted' must be set in DropResponse");
  }
}
