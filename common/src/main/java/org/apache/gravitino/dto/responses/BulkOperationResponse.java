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
import org.apache.commons.lang3.StringUtils;

/** Represents the result of a bulk operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class BulkOperationResponse extends BaseResponse {

  @JsonProperty("succeeded")
  private final String[] succeeded;

  @JsonProperty("failed")
  private final BulkOperationFailureDTO[] failed;

  /**
   * Creates a new BulkOperationResponse.
   *
   * @param succeeded The successfully processed entity names.
   * @param failed The failed entity names and reasons.
   */
  public BulkOperationResponse(String[] succeeded, BulkOperationFailureDTO[] failed) {
    this.succeeded = succeeded;
    this.failed = failed;
  }

  /** Default constructor for BulkOperationResponse. (Used for Jackson deserialization.) */
  public BulkOperationResponse() {
    this(null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(succeeded != null, "\"succeeded\" must not be null");
    Preconditions.checkArgument(failed != null, "\"failed\" must not be null");
    Arrays.stream(succeeded)
        .forEach(
            name ->
                Preconditions.checkArgument(
                    StringUtils.isNotBlank(name), "\"succeeded\" cannot contain blank name"));
    Arrays.stream(failed).forEach(BulkOperationFailureDTO::validate);
  }
}
