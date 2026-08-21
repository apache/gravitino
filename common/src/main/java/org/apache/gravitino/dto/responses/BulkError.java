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

/** Represents an item-level error in a bulk response. */
@Getter
@ToString
@EqualsAndHashCode
public class BulkError {

  @JsonProperty("index")
  private final int index;

  @Nullable
  @JsonProperty("name")
  private final String name;

  @JsonProperty("code")
  private final int code;

  @JsonProperty("type")
  private final String type;

  @JsonProperty("message")
  private final String message;

  /**
   * Creates a new BulkError.
   *
   * @param index The zero-based index of the failed request item.
   * @param name The name of the failed request item.
   * @param code The Gravitino error code.
   * @param type The error type.
   * @param message The error message.
   */
  public BulkError(int index, @Nullable String name, int code, String type, String message) {
    this.index = index;
    this.name = name;
    this.code = code;
    this.type = type;
    this.message = message;
  }

  /** Default constructor for BulkError. (Used for Jackson deserialization.) */
  public BulkError() {
    this(-1, null, 0, null, null);
  }

  /** Validates the bulk error. */
  public void validate() {
    Preconditions.checkArgument(index >= 0, "index must be >= 0");
    Preconditions.checkArgument(code > 0, "code must be > 0");
    Preconditions.checkArgument(StringUtils.isNotBlank(type), "type must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(message), "message must not be blank");
  }
}
