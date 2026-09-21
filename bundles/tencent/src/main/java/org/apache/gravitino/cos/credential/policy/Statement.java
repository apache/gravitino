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

package org.apache.gravitino.cos.credential.policy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/** A Tencent Cloud CAM policy statement. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Statement {

  @JsonProperty("effect")
  private String effect;

  @JsonProperty("action")
  private List<String> actions;

  @JsonProperty("resource")
  private List<String> resources;

  @JsonProperty("condition")
  private Condition condition;

  private Statement(Builder builder) {
    this.effect = builder.effect;
    this.actions = builder.actions;
    this.resources = builder.resources;
    this.condition = builder.condition;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String effect;
    private final List<String> actions = new ArrayList<>();
    private final List<String> resources = new ArrayList<>();
    private Condition condition;
    // Lazily accumulates cos:prefix values across calls to addStringLikePrefix.
    private StringLike.Builder stringLikeBuilder;

    public Builder effect(String effect) {
      this.effect = effect;
      return this;
    }

    public Builder addAction(String action) {
      this.actions.add(action);
      return this;
    }

    public Builder addResource(String resource) {
      this.resources.add(resource);
      return this;
    }

    public Builder condition(Condition condition) {
      this.condition = condition;
      return this;
    }

    /** Appends a cos:prefix pattern to the statement's string_like condition. */
    public Builder addStringLikePrefix(String prefix) {
      if (stringLikeBuilder == null) {
        stringLikeBuilder = StringLike.builder();
      }
      stringLikeBuilder.addPrefix(prefix);
      return this;
    }

    public Statement build() {
      // Explicit condition() wins; only auto-assemble from accumulated prefixes if unset.
      if (condition == null && stringLikeBuilder != null) {
        condition = Condition.builder().stringLike(stringLikeBuilder.build()).build();
      }
      return new Statement(this);
    }
  }

  @SuppressWarnings("unused")
  public String getEffect() {
    return effect;
  }

  @SuppressWarnings("unused")
  public List<String> getActions() {
    return actions;
  }

  @SuppressWarnings("unused")
  public List<String> getResources() {
    return resources;
  }

  @SuppressWarnings("unused")
  public Condition getCondition() {
    return condition;
  }
}
