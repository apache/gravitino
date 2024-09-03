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
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response for a set operation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class SetResponse extends BaseResponse {

  @JsonProperty("set")
  private final boolean set;

  /**
   * Constructor for SetResponse.
   *
   * @param set Whether the set operation was successful.
   */
  public SetResponse(boolean set) {
    super(0);
    this.set = set;
  }

  /** Default constructor for SetResponse (used by Jackson deserializer). */
  public SetResponse() {
    super(0);
    this.set = false;
  }

  /**
   * Returns whether the set operation was successful.
   *
   * @return True if the set operation was successful, otherwise false.
   */
  public boolean set() {
    return set;
  }
}
