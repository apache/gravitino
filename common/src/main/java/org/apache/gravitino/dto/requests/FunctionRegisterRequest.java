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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to register a function. */
@Getter
@EqualsAndHashCode
@ToString
@Builder(setterPrefix = "with")
@NoArgsConstructor
@AllArgsConstructor
public class FunctionRegisterRequest implements RESTRequest {

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

  @Nullable
  @JsonProperty("returnColumns")
  private FunctionColumnDTO[] returnColumns;

  @JsonProperty("definitions")
  private FunctionDefinitionDTO[] definitions;

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException if the request is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(functionType != null, "\"functionType\" field is required");
    Preconditions.checkArgument(
        definitions != null && definitions.length > 0,
        "\"definitions\" field is required and cannot be empty");

    if (functionType == FunctionType.TABLE) {
      Preconditions.checkArgument(
          returnColumns != null && returnColumns.length > 0,
          "\"returnColumns\" is required for TABLE function type");
    } else if (functionType == FunctionType.SCALAR || functionType == FunctionType.AGGREGATE) {
      Preconditions.checkArgument(
          returnType != null, "\"returnType\" is required for SCALAR or AGGREGATE function type");
    } else {
      throw new IllegalArgumentException("Unsupported function type: " + functionType);
    }
  }
}
