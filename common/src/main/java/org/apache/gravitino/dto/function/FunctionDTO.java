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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;

/** Represents a Function DTO (Data Transfer Object). */
@NoArgsConstructor
@AllArgsConstructor
@Builder(setterPrefix = "with")
@EqualsAndHashCode
public class FunctionDTO implements Function {

  @JsonProperty("name")
  private String name;

  @JsonProperty("functionType")
  private FunctionType functionType;

  @JsonProperty("deterministic")
  private boolean deterministic;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("returnType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type returnType;

  @JsonProperty("returnColumns")
  private FunctionColumnDTO[] returnColumns;

  @JsonProperty("definitions")
  private FunctionDefinitionDTO[] definitions;

  @JsonProperty("audit")
  private AuditDTO audit;

  @Override
  public String name() {
    return name;
  }

  @Override
  public FunctionType functionType() {
    return functionType;
  }

  @Override
  public boolean deterministic() {
    return deterministic;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Type returnType() {
    return returnType;
  }

  @Override
  public FunctionColumn[] returnColumns() {
    if (returnColumns == null) {
      return new FunctionColumn[0];
    }
    return Arrays.stream(returnColumns)
        .map(FunctionColumnDTO::toFunctionColumn)
        .toArray(FunctionColumn[]::new);
  }

  @Override
  public FunctionDefinition[] definitions() {
    if (definitions == null) {
      return new FunctionDefinition[0];
    }
    return definitions;
  }

  @Override
  public Audit auditInfo() {
    return audit;
  }

  @Override
  public String toString() {
    return "FunctionDTO{"
        + "name='"
        + name
        + '\''
        + ", functionType="
        + functionType
        + ", deterministic="
        + deterministic
        + ", comment='"
        + comment
        + '\''
        + ", returnType="
        + returnType
        + ", returnColumns="
        + Arrays.toString(returnColumns)
        + ", definitions="
        + Arrays.toString(definitions)
        + ", audit="
        + audit
        + '}';
  }
}
