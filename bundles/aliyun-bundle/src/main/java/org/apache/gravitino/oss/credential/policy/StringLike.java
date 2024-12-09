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
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StringLike {

  @JsonProperty("oss:Prefix")
  private List<String> prefix;

  private StringLike(Builder builder) {
    this.prefix = builder.prefix;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> prefix = new ArrayList<>();

    public Builder addPrefix(String prefix) {
      this.prefix.add(prefix);
      return this;
    }

    public StringLike build() {
      return new StringLike(this);
    }
  }

  @SuppressWarnings("unused")
  public List<String> getPrefix() {
    return prefix;
  }
}
