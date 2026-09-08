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

/**
 * Tencent Cloud CAM policy document used as the {@code Policy} parameter of {@code sts:AssumeRole}.
 * The serialized JSON follows the format described in the official Tencent Cloud CAM documentation
 * (e.g. {@code {"version":"2.0","statement":[...]}}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Policy {

  @JsonProperty("version")
  private String version;

  @JsonProperty("statement")
  private List<Statement> statements;

  private Policy(Builder builder) {
    this.version = builder.version;
    this.statements = builder.statements;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String version;
    private final List<Statement> statements = new ArrayList<>();

    public Builder version(String version) {
      this.version = version;
      return this;
    }

    public Builder addStatement(Statement statement) {
      this.statements.add(statement);
      return this;
    }

    public Policy build() {
      return new Policy(this);
    }
  }

  @SuppressWarnings("unused")
  public String getVersion() {
    return version;
  }

  @SuppressWarnings("unused")
  public List<Statement> getStatements() {
    return statements;
  }
}
