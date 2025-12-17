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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.FunctionSignatureDTO;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rest.RESTRequest;

/** Request to register a function. */
@Getter
@EqualsAndHashCode
@ToString
public class FunctionRegisterRequest implements RESTRequest {

  @JsonProperty("signature")
  private FunctionSignatureDTO signature;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("type")
  private FunctionType type;

  @JsonProperty("deterministic")
  private boolean deterministic;

  @JsonProperty("returnType")
  @JsonSerialize(using = JsonUtils.TypeSerializer.class)
  @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
  private Type returnType;

  @JsonProperty("returnColumns")
  private FunctionColumnDTO[] returnColumns;

  @JsonProperty("impls")
  private FunctionImplDTO[] impls;

  private FunctionRegisterRequest() {}

  /**
   * Creates a request to register a function.
   *
   * @param signature Function signature.
   * @param comment Optional comment.
   * @param type Function type.
   * @param deterministic Whether deterministic.
   * @param returnType Return type for scalar/aggregate functions.
   * @param returnColumns Return columns for table functions.
   * @param impls Function implementations.
   */
  public FunctionRegisterRequest(
      FunctionSignatureDTO signature,
      String comment,
      FunctionType type,
      boolean deterministic,
      Type returnType,
      FunctionColumnDTO[] returnColumns,
      FunctionImplDTO[] impls) {
    this.signature = signature;
    this.comment = comment;
    this.type = type;
    this.deterministic = deterministic;
    this.returnType = returnType;
    this.returnColumns = returnColumns;
    this.impls = impls;
  }

  /** {@inheritDoc} */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(signature != null, "\"signature\" must be provided");
    signature.validate();
    Preconditions.checkArgument(type != null, "\"type\" must be provided");
    Preconditions.checkArgument(impls != null && impls.length > 0, "\"impls\" must not be empty");
    Arrays.stream(impls).forEach(FunctionImplDTO::validate);
    if (type == FunctionType.TABLE) {
      Preconditions.checkArgument(
          returnColumns != null && returnColumns.length > 0,
          "\"returnColumns\" must not be empty for table function");
      Arrays.stream(returnColumns).forEach(FunctionColumnDTO::validate);
      Preconditions.checkArgument(
          returnType == null, "\"returnType\" must be null for table function");
    } else {
      Preconditions.checkArgument(returnType != null, "\"returnType\" must be provided");
    }
    FunctionParamDTO[] params = signature.getFunctionParams();
    if (params != null) {
      Arrays.stream(params).forEach(FunctionParamDTO::validate);
    }
  }
}
