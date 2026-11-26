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
import org.apache.gravitino.dto.semantic.SemanticModelRevisionDTO;

/** A response containing the visible Semantic Model revisions, newest first. */
public class SemanticModelRevisionListResponse extends BaseResponse {

  @JsonProperty("revisions")
  private final SemanticModelRevisionDTO[] revisions;

  /** Default constructor for Jackson deserialization. */
  public SemanticModelRevisionListResponse() {
    this(null);
  }

  /**
   * Creates a response with the revision summaries.
   *
   * @param revisions The revision summaries, newest first.
   */
  public SemanticModelRevisionListResponse(SemanticModelRevisionDTO[] revisions) {
    super(0);
    this.revisions = revisions;
  }

  /**
   * Returns the revision summaries.
   *
   * @return The revision summaries, newest first.
   */
  public SemanticModelRevisionDTO[] revisions() {
    return revisions;
  }

  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(revisions != null, "revisions must not be null");
  }
}
