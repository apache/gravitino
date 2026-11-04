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
import lombok.Getter;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;

/** Effective policies for a batch of metadata objects. */
@Getter
public class BulkMetadataObjectPolicyListResponse extends BaseResponse {
  @JsonProperty("results")
  private final Result[] results;

  /**
   * Creates a bulk policy response.
   *
   * @param results the results in request order
   */
  public BulkMetadataObjectPolicyListResponse(Result[] results) {
    super(0);
    this.results = results;
  }

  /** Creates a response for Jackson deserialization. */
  public BulkMetadataObjectPolicyListResponse() {
    this(null);
  }

  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(results != null, "results must not be null");
    for (Result result : results) {
      Preconditions.checkArgument(
          result != null && result.metadataObject != null && result.policies != null,
          "result fields must not be null");
      result.policies.validate();
    }
  }

  /** Visible effective policies for one metadata object. */
  @Getter
  public static class Result {
    @JsonProperty("metadataObject")
    private final MetadataObjectDTO metadataObject;

    @JsonProperty("policies")
    private final MetadataObjectPolicyListResponse policies;

    /**
     * Creates one object's result.
     *
     * @param metadataObject the requested object
     * @param policies visible effective policies, including the visibility indicator
     */
    public Result(MetadataObjectDTO metadataObject, MetadataObjectPolicyListResponse policies) {
      this.metadataObject = metadataObject;
      this.policies = policies;
    }

    /** Creates a result for Jackson deserialization. */
    public Result() {
      this(null, null);
    }
  }
}
