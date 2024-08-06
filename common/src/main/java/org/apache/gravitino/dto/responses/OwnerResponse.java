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
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.authorization.OwnerDTO;

/** Represents a response containing owner information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class OwnerResponse extends BaseResponse {

  @Nullable
  @JsonProperty("owner")
  private final OwnerDTO owner;

  /**
   * Constructor for OwnerResponse.
   *
   * @param owner The owner data transfer object.
   */
  public OwnerResponse(OwnerDTO owner) {
    super(0);
    this.owner = owner;
  }

  /** Default constructor for OwnerResponse. (Used for Jackson deserialization.) */
  public OwnerResponse() {
    super(0);
    this.owner = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    if (owner != null) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(owner.name()), "owner 'name' must not be null or empty");
      Preconditions.checkArgument(owner.type() != null, "owner 'type' must not be null");
    }
  }
}
