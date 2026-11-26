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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.glossary.TermBindingDTO;
import org.apache.gravitino.glossary.TermBindingPage;

/** One page of visible glossary bindings in stable target order. */
public class TermBindingListResponse extends BaseResponse {
  @JsonProperty("bindings")
  private TermBindingDTO[] bindings;

  @JsonProperty("nextPageToken")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  private String nextPageToken;

  /** Creates an empty response for deserialization. */
  public TermBindingListResponse() {}

  /** Creates a page with an optional opaque continuation, including for an empty page. */
  public TermBindingListResponse(TermBindingDTO[] bindings, @Nullable String nextPageToken) {
    super(0);
    Preconditions.checkArgument(bindings != null, "bindings must not be null");
    this.bindings = bindings.clone();
    this.nextPageToken = nextPageToken;
  }

  /** Returns this page's authorized bindings. */
  public TermBindingDTO[] getBindings() {
    return bindings == null ? null : bindings.clone();
  }

  /** Returns the continuation, or null when the scan is exhausted. */
  @Nullable
  public String getNextPageToken() {
    return nextPageToken;
  }

  /** Validates the response envelope and its bindings. */
  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(bindings != null, "bindings must not be null");
    Preconditions.checkArgument(
        bindings.length <= TermBindingPage.MAX_PAGE_SIZE,
        "A binding page cannot exceed %s results",
        TermBindingPage.MAX_PAGE_SIZE);
    for (TermBindingDTO binding : bindings) {
      Preconditions.checkArgument(binding != null, "bindings must not contain null");
      binding.validate();
    }
    Preconditions.checkArgument(
        nextPageToken == null || StringUtils.isNotBlank(nextPageToken),
        "nextPageToken must not be blank");
  }
}
