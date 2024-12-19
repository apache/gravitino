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

package org.apache.gravitino.oss.credential.policy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Condition {

  @JsonProperty("StringLike")
  private StringLike stringLike;

  private Condition(Builder builder) {
    this.stringLike = builder.stringLike;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private StringLike stringLike;

    public Builder stringLike(StringLike stringLike) {
      this.stringLike = stringLike;
      return this;
    }

    public Condition build() {
      return new Condition(this);
    }
  }

  @SuppressWarnings("unused")
  public StringLike getStringLike() {
    return stringLike;
  }
}
