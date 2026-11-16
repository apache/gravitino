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
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** A request to set a domain's parent. */
@Getter
public class DomainParentSetRequest implements RESTRequest {

  @JsonProperty("parent")
  private final String parent;

  /** Creates a request for Jackson deserialization. */
  public DomainParentSetRequest() {
    this(null);
  }

  /**
   * Creates a set request.
   *
   * @param parent The new parent.
   */
  public DomainParentSetRequest(String parent) {
    this.parent = parent;
  }

  /** Validates the requested value. */
  @Override
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(parent), "parent must not be blank");
  }
}
