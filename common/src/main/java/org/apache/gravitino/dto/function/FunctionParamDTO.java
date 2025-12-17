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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rest.RESTRequest;

/** DTO for {@link FunctionParam}. */
@EqualsAndHashCode
@ToString
public class FunctionParamDTO implements FunctionParam, RESTRequest {

  @JsonProperty("name")
  private String name;

  @JsonProperty("dataType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type dataType;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("defaultValue")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonSerialize(using = JsonUtils.ColumnDefaultValueSerializer.class)
  @JsonDeserialize(using = JsonUtils.ColumnDefaultValueDeserializer.class)
  private Expression defaultValue = FunctionParam.DEFAULT_VALUE_NOT_SET;

  /** Default constructor for Jackson. */
  private FunctionParamDTO() {}

  /**
   * Creates a function parameter DTO.
   *
   * @param name Parameter name.
   * @param dataType Parameter data type.
   * @param comment Optional parameter comment.
   * @param defaultValue Default value; if null, {@link FunctionParam#DEFAULT_VALUE_NOT_SET} is
   *     used.
   */
  public FunctionParamDTO(String name, Type dataType, String comment, Expression defaultValue) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.defaultValue = defaultValue == null ? FunctionParam.DEFAULT_VALUE_NOT_SET : defaultValue;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public Type dataType() {
    return dataType;
  }

  /** {@inheritDoc} */
  @Override
  public String comment() {
    return comment;
  }

  /** {@inheritDoc} */
  @Override
  public Expression defaultValue() {
    return defaultValue;
  }

  /** {@inheritDoc} */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"name\" field is required");
    Preconditions.checkArgument(dataType != null, "\"dataType\" field is required");
  }
}
