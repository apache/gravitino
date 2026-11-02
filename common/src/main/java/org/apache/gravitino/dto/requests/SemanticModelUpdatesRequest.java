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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents an atomic request to alter a Semantic Model. */
@Getter
@EqualsAndHashCode
@ToString
public class SemanticModelUpdatesRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<SemanticModelUpdateRequest> updates;

  /** Default constructor for Jackson deserialization. */
  public SemanticModelUpdatesRequest() {
    this(null);
  }

  /**
   * Creates an atomic Semantic Model updates request.
   *
   * @param updates The non-empty list of updates to apply.
   */
  public SemanticModelUpdatesRequest(List<SemanticModelUpdateRequest> updates) {
    this.updates = updates;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(updates != null, "updates must not be null");
    Preconditions.checkArgument(!updates.isEmpty(), "updates must not be empty");
    for (SemanticModelUpdateRequest update : updates) {
      Preconditions.checkArgument(update != null, "update must not be null");
      update.validate();
    }
  }
}
