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
public class Statement {

  @JsonProperty("Effect")
  private String effect;

  @JsonProperty("Action")
  private List<String> actions;

  @JsonProperty("Resource")
  private List<String> resources;

  @JsonProperty("Condition")
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
    private List<String> actions = new ArrayList<>();
    private List<String> resources = new ArrayList<>();
    private Condition condition;

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

    public Statement build() {
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
