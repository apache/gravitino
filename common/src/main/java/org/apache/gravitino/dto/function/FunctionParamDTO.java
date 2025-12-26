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

import static org.apache.gravitino.dto.util.DTOConverters.toFunctionArg;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** DTO for function parameter. */
@Getter
@EqualsAndHashCode
public class FunctionParamDTO implements FunctionParam {

  @JsonProperty("name")
  private String name;

  @JsonProperty("dataType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @JsonProperty("defaultValue")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonSerialize(using = JsonUtils.ColumnDefaultValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.ColumnDefaultValueDeserializer.class)
  private Expression defaultValue = DEFAULT_VALUE_NOT_SET;

  private FunctionParamDTO() {}

  private FunctionParamDTO(String name, Type dataType, String comment, Expression defaultValue) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.defaultValue = defaultValue == null ? DEFAULT_VALUE_NOT_SET : defaultValue;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Type dataType() {
    return dataType;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Expression defaultValue() {
    return defaultValue;
  }

  /**
   * Convert this DTO to a {@link FunctionParam} instance.
   *
   * @return The function parameter.
   */
  public FunctionParam toFunctionParam() {
    return FunctionParams.of(name, dataType, comment, defaultValue());
  }

  /**
   * Create a {@link FunctionParamDTO} from a {@link FunctionParam} instance.
   *
   * @param param The function parameter.
   * @return The function parameter DTO.
   */
  public static FunctionParamDTO fromFunctionParam(FunctionParam param) {
    return new FunctionParamDTO(
        param.name(),
        param.dataType(),
        param.comment(),
        (param.defaultValue() == null || param.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET))
            ? Column.DEFAULT_VALUE_NOT_SET
            : toFunctionArg(param.defaultValue()));
  }

  @Override
  public String toString() {
    return "FunctionParamDTO{"
        + "name='"
        + name
        + '\''
        + ", dataType="
        + dataType
        + ", comment='"
        + comment
        + '\''
        + ", defaultValue="
        + defaultValue
        + '}';
  }

  /** Builder for {@link FunctionParamDTO}. */
  public static class Builder {
    private String name;
    private Type dataType;
    private String comment;
    private Expression defaultValue;

    /**
     * Set the parameter name.
     *
     * @param name The parameter name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the parameter data type.
     *
     * @param dataType The parameter data type.
     * @return This builder.
     */
    public Builder withDataType(Type dataType) {
      this.dataType = dataType;
      return this;
    }

    /**
     * Set the parameter comment.
     *
     * @param comment The parameter comment.
     * @return This builder.
     */
    public Builder withComment(String comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Set the parameter default value.
     *
     * @param defaultValue The parameter default value.
     * @return This builder.
     */
    public Builder withDefaultValue(Expression defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    /**
     * Build the {@link FunctionParamDTO}.
     *
     * @return The function parameter DTO.
     */
    public FunctionParamDTO build() {
      return new FunctionParamDTO(name, dataType, comment, defaultValue);
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
