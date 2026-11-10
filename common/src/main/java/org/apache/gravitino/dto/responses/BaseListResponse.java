/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Base class of the list responses of the REST API. A list response can carry one page of a list;
 * {@code nextPageToken} is serialized only when more entries remain, so an unpaged response is
 * unchanged on the wire.
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public abstract class BaseListResponse extends BaseResponse {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("nextPageToken")
  @Nullable
  private String nextPageToken;

  /**
   * Creates a list response with the given status code.
   *
   * @param code The status code.
   */
  protected BaseListResponse(int code) {
    super(code);
  }

  /** Creates a list response for Jackson deserialization. */
  protected BaseListResponse() {
    super();
  }

  /**
   * @return The token for the next page, or null when no further entry remains or the request was
   *     not paged.
   */
  @Nullable
  public String nextPageToken() {
    return nextPageToken;
  }

  /**
   * Sets the token for the next page.
   *
   * @param nextPageToken The token, or null when no further entry remains.
   * @param <R> The concrete response type.
   * @return This response.
   */
  @SuppressWarnings("unchecked")
  public <R extends BaseListResponse> R withNextPageToken(@Nullable String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return (R) this;
  }
}
