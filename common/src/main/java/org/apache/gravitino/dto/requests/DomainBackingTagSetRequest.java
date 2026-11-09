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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.rest.RESTRequest;

/** A request to set a domain's backingTag. */
@Getter
public class DomainBackingTagSetRequest implements RESTRequest {

  @JsonProperty("backingTag")
  @Nullable
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private DomainBackingTagDTO backingTag;

  @JsonIgnore
  @Getter(AccessLevel.NONE)
  private boolean backingTagSpecified;

  /** Creates a request for Jackson deserialization. */
  public DomainBackingTagSetRequest() {
    // Jackson invokes the setter only when the backingTag field is present.
  }

  /**
   * Creates a set request.
   *
   * @param backingTag The new backingTag. Null removes the backing tag.
   */
  public DomainBackingTagSetRequest(@Nullable DomainBackingTagDTO backingTag) {
    setBackingTag(backingTag);
  }

  /**
   * Records an explicitly supplied binding, including null to remove it.
   *
   * @param backingTag The binding, or null to remove the current binding.
   */
  @JsonSetter("backingTag")
  public void setBackingTag(@Nullable DomainBackingTagDTO backingTag) {
    this.backingTag = backingTag;
    this.backingTagSpecified = true;
  }

  /** Validates the requested value. */
  @Override
  public void validate() {
    Preconditions.checkArgument(
        backingTagSpecified,
        "backingTag must be explicitly provided; use null to remove the binding");
    if (backingTag != null) {
      backingTag.validate();
    }
  }
}
