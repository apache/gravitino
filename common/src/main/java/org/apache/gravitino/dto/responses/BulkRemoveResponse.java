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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a bulk remove response. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class BulkRemoveResponse extends BaseResponse {

  @JsonProperty("names")
  private final String[] names;

  @JsonProperty("errors")
  private final BulkError[] errors;

  @JsonProperty("summary")
  private final BulkSummary summary;

  /**
   * Creates a new BulkRemoveResponse.
   *
   * @param names The successfully removed names.
   * @param errors The item-level errors.
   * @param summary The summary counts.
   */
  public BulkRemoveResponse(String[] names, BulkError[] errors, BulkSummary summary) {
    super(0);
    this.names = names;
    this.errors = errors;
    this.summary = summary;
  }

  /** Default constructor for BulkRemoveResponse. (Used for Jackson deserialization.) */
  public BulkRemoveResponse() {
    this(null, null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(names != null, "names must not be null");
    Preconditions.checkArgument(errors != null, "errors must not be null");
    Preconditions.checkArgument(summary != null, "summary must not be null");
    Arrays.stream(errors).forEach(BulkError::validate);
    summary.validate();
  }
}
