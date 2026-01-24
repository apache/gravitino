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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a function. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = FunctionUpdateRequest.UpdateCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = FunctionUpdateRequest.AddDefinitionRequest.class,
      name = "addDefinition"),
  @JsonSubTypes.Type(
      value = FunctionUpdateRequest.RemoveDefinitionRequest.class,
      name = "removeDefinition"),
  @JsonSubTypes.Type(value = FunctionUpdateRequest.AddImplRequest.class, name = "addImpl"),
  @JsonSubTypes.Type(value = FunctionUpdateRequest.UpdateImplRequest.class, name = "updateImpl"),
  @JsonSubTypes.Type(value = FunctionUpdateRequest.RemoveImplRequest.class, name = "removeImpl")
})
public interface FunctionUpdateRequest extends RESTRequest {

  /**
   * Returns the function change.
   *
   * @return the function change.
   */
  FunctionChange functionChange();

  /** The function update request for updating the comment of a function. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateCommentRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    @Override
    public FunctionChange functionChange() {
      return FunctionChange.updateComment(newComment);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      // newComment can be null or empty to clear the comment
    }
  }

  /** The function update request for adding a definition to a function. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class AddDefinitionRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("definition")
    private final FunctionDefinitionDTO definition;

    @Override
    public FunctionChange functionChange() {
      FunctionDefinition def = definition.toFunctionDefinition();
      return FunctionChange.addDefinition(def);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          definition != null, "\"definition\" field is required and cannot be null");
    }
  }

  /** The function update request for removing a definition from a function. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveDefinitionRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("parameters")
    private final FunctionParamDTO[] parameters;

    @Override
    public FunctionChange functionChange() {
      FunctionParam[] params = new FunctionParam[parameters == null ? 0 : parameters.length];
      if (parameters != null) {
        for (int i = 0; i < parameters.length; i++) {
          params[i] = parameters[i].toFunctionParam();
        }
      }
      return FunctionChange.removeDefinition(params);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          parameters != null, "\"parameters\" field is required and cannot be null");
    }
  }

  /** The function update request for adding an implementation to a definition. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class AddImplRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("parameters")
    private final FunctionParamDTO[] parameters;

    @Getter
    @JsonProperty("implementation")
    private final FunctionImplDTO implementation;

    @Override
    public FunctionChange functionChange() {
      FunctionParam[] params = new FunctionParam[parameters == null ? 0 : parameters.length];
      if (parameters != null) {
        for (int i = 0; i < parameters.length; i++) {
          params[i] = parameters[i].toFunctionParam();
        }
      }
      FunctionImpl impl = implementation.toFunctionImpl();
      return FunctionChange.addImpl(params, impl);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          parameters != null, "\"parameters\" field is required and cannot be null");
      Preconditions.checkArgument(
          implementation != null, "\"implementation\" field is required and cannot be null");
    }
  }

  /** The function update request for updating an implementation in a definition. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class UpdateImplRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("parameters")
    private final FunctionParamDTO[] parameters;

    @Getter
    @JsonProperty("runtime")
    private final String runtime;

    @Getter
    @JsonProperty("implementation")
    private final FunctionImplDTO implementation;

    @Override
    public FunctionChange functionChange() {
      FunctionParam[] params = new FunctionParam[parameters == null ? 0 : parameters.length];
      if (parameters != null) {
        for (int i = 0; i < parameters.length; i++) {
          params[i] = parameters[i].toFunctionParam();
        }
      }
      FunctionImpl.RuntimeType runtimeType = FunctionImpl.RuntimeType.fromString(runtime);
      FunctionImpl impl = implementation.toFunctionImpl();
      return FunctionChange.updateImpl(params, runtimeType, impl);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          parameters != null, "\"parameters\" field is required and cannot be null");
      Preconditions.checkArgument(
          runtime != null, "\"runtime\" field is required and cannot be null");
      Preconditions.checkArgument(
          implementation != null, "\"implementation\" field is required and cannot be null");
    }
  }

  /** The function update request for removing an implementation from a definition. */
  @EqualsAndHashCode
  @NoArgsConstructor(force = true)
  @AllArgsConstructor
  @ToString
  class RemoveImplRequest implements FunctionUpdateRequest {

    @Getter
    @JsonProperty("parameters")
    private final FunctionParamDTO[] parameters;

    @Getter
    @JsonProperty("runtime")
    private final String runtime;

    @Override
    public FunctionChange functionChange() {
      FunctionParam[] params = new FunctionParam[parameters == null ? 0 : parameters.length];
      if (parameters != null) {
        for (int i = 0; i < parameters.length; i++) {
          params[i] = parameters[i].toFunctionParam();
        }
      }
      FunctionImpl.RuntimeType runtimeType = FunctionImpl.RuntimeType.fromString(runtime);
      return FunctionChange.removeImpl(params, runtimeType);
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          parameters != null, "\"parameters\" field is required and cannot be null");
      Preconditions.checkArgument(
          runtime != null, "\"runtime\" field is required and cannot be null");
    }
  }
}
