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
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.glossary.TermChange;
import org.apache.gravitino.rest.RESTRequest;

/** Ordered changes applied together, optionally against an expected revision. */
public class TermUpdatesRequest implements RESTRequest {
  @Getter
  @Nullable
  @JsonProperty("expectedRevision")
  private String expectedRevision;

  @JsonProperty("updates")
  private TermUpdateRequest[] updates;

  /** Creates an empty request for deserialization. */
  public TermUpdatesRequest() {}

  /** Creates an atomic update with an optional revision guard. */
  public TermUpdatesRequest(@Nullable String expectedRevision, TermUpdateRequest... updates) {
    this.expectedRevision = expectedRevision;
    this.updates = updates.clone();
    validate();
  }

  /** Returns the validated changes in request order. */
  public TermChange[] changes() {
    validate();
    return Arrays.stream(updates).map(TermUpdateRequest::toChange).toArray(TermChange[]::new);
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        updates != null && updates.length > 0 && Arrays.stream(updates).allMatch(Objects::nonNull),
        "updates must contain one or more changes");
  }
}
