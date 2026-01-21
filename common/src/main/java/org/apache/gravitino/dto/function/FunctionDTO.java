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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.Audit;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;

/** Represents a Function DTO (Data Transfer Object). */
@Getter
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

  private FunctionDTO() {}

  private FunctionDTO(
      String name,
      FunctionType functionType,
      boolean deterministic,
      String comment,
      Type returnType,
      FunctionColumnDTO[] returnColumns,
      FunctionDefinitionDTO[] definitions,
      AuditDTO audit) {
    this.name = name;
    this.functionType = functionType;
    this.deterministic = deterministic;
    this.comment = comment;
    this.returnType = returnType;
    this.returnColumns = returnColumns;
    this.definitions = definitions;
    this.audit = audit;
  }

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

  /** Builder for {@link FunctionDTO}. */
  public static class Builder {
    private String name;
    private FunctionType functionType;
    private boolean deterministic;
    private String comment;
    private Type returnType;
    private FunctionColumnDTO[] returnColumns;
    private FunctionDefinitionDTO[] definitions;
    private AuditDTO audit;

    /**
     * Set the function name.
     *
     * @param name The function name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the function type.
     *
     * @param functionType The function type.
     * @return This builder.
     */
    public Builder withFunctionType(FunctionType functionType) {
      this.functionType = functionType;
      return this;
    }

    /**
     * Set whether the function is deterministic.
     *
     * @param deterministic Whether the function is deterministic.
     * @return This builder.
     */
    public Builder withDeterministic(boolean deterministic) {
      this.deterministic = deterministic;
      return this;
    }

    /**
     * Set the function comment.
     *
     * @param comment The function comment.
     * @return This builder.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Set the return type.
     *
     * @param returnType The return type.
     * @return This builder.
     */
    public Builder withReturnType(Type returnType) {
      this.returnType = returnType;
      return this;
    }

    /**
     * Set the return columns for table-valued functions.
     *
     * @param returnColumns The return columns.
     * @return This builder.
     */
    public Builder withReturnColumns(FunctionColumnDTO[] returnColumns) {
      this.returnColumns = returnColumns;
      return this;
    }

    /**
     * Set the function definitions.
     *
     * @param definitions The function definitions.
     * @return This builder.
     */
    public Builder withDefinitions(FunctionDefinitionDTO[] definitions) {
      this.definitions = definitions;
      return this;
    }

    /**
     * Set the audit information.
     *
     * @param audit The audit information.
     * @return This builder.
     */
    public Builder withAudit(AuditDTO audit) {
      this.audit = audit;
      return this;
    }

    /**
     * Build the {@link FunctionDTO}.
     *
     * @return The function DTO.
     */
    public FunctionDTO build() {
      return new FunctionDTO(
          name,
          functionType,
          deterministic,
          comment,
          returnType,
          returnColumns,
          definitions,
          audit);
    }
  }

  /**
   * Create a new builder.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }
}
