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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents summary counts for a bulk response. */
@Getter
@ToString
@EqualsAndHashCode
public class BulkSummary {

  @JsonProperty("total")
  private final int total;

  @JsonProperty("succeeded")
  private final int succeeded;

  @JsonProperty("failed")
  private final int failed;

  /**
   * Creates a new BulkSummary.
   *
   * @param total The total number of request items.
   * @param succeeded The number of succeeded request items.
   * @param failed The number of failed request items.
   */
  public BulkSummary(int total, int succeeded, int failed) {
    this.total = total;
    this.succeeded = succeeded;
    this.failed = failed;
  }

  /** Default constructor for BulkSummary. (Used for Jackson deserialization.) */
  public BulkSummary() {
    this(0, 0, 0);
  }

  /** Validates the bulk summary. */
  public void validate() {
    Preconditions.checkArgument(total >= 0, "total must be >= 0");
    Preconditions.checkArgument(succeeded >= 0, "succeeded must be >= 0");
    Preconditions.checkArgument(failed >= 0, "failed must be >= 0");
    Preconditions.checkArgument(total == succeeded + failed, "total must equal succeeded + failed");
  }
}
