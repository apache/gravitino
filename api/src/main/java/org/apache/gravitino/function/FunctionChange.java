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
package org.apache.gravitino.function;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** Represents a change that can be applied to a function. */
@Evolving
public interface FunctionChange {
  /** An empty array of parameters. */
  FunctionParam[] EMPTY_PARAMS = new FunctionParam[0];

  /**
   * Create a {@link FunctionChange} to update the comment of a function.
   *
   * @param newComment The new comment value.
   * @return The change instance.
   */
  static FunctionChange updateComment(String newComment) {
    return new UpdateComment(newComment);
  }

  /**
   * Create a {@link FunctionChange} to add a new definition (overload) to a function.
   *
   * @param definition The new definition to add.
   * @return The change instance.
   */
  static FunctionChange addDefinition(FunctionDefinition definition) {
    return new AddDefinition(definition);
  }

  /**
   * Create a {@link FunctionChange} to remove an existing definition (overload) from a function.
   *
   * @param parameters The parameters that identify the definition to remove.
   * @return The change instance.
   */
  static FunctionChange removeDefinition(FunctionParam[] parameters) {
    return new RemoveDefinition(parameters);
  }

  /**
   * Create a {@link FunctionChange} to add an implementation to a specific definition.
   *
   * @param parameters The parameters that identify the definition to update.
   * @param implementation The implementation to add.
   * @return The change instance.
   */
  static FunctionChange addImpl(FunctionParam[] parameters, FunctionImpl implementation) {
    return new AddImpl(parameters, implementation);
  }

  /**
   * Create a {@link FunctionChange} to update an implementation for a specific definition and
   * runtime.
   *
   * @param parameters The parameters that identify the definition to update.
   * @param runtime The runtime that identifies the implementation to replace.
   * @param implementation The new implementation.
   * @return The change instance.
   */
  static FunctionChange updateImpl(
      FunctionParam[] parameters, FunctionImpl.RuntimeType runtime, FunctionImpl implementation) {
    return new UpdateImpl(parameters, runtime, implementation);
  }

  /**
   * Create a {@link FunctionChange} to remove an implementation for a specific definition and
   * runtime.
   *
   * @param parameters The parameters that identify the definition to update.
   * @param runtime The runtime that identifies the implementation to remove.
   * @return The change instance.
   */
  static FunctionChange removeImpl(FunctionParam[] parameters, FunctionImpl.RuntimeType runtime) {
    return new RemoveImpl(parameters, runtime);
  }

  /** A {@link FunctionChange} to update the comment of a function. */
  final class UpdateComment implements FunctionChange {
    private final String newComment;

    UpdateComment(String newComment) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment), "New comment cannot be null or empty");
      this.newComment = newComment;
    }

    /**
     * @return The new comment of the function.
     */
    public String newComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof UpdateComment)) {
        return false;
      }
      UpdateComment that = (UpdateComment) obj;
      return Objects.equals(newComment, that.newComment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    @Override
    public String toString() {
      return "UpdateComment{newComment='" + newComment + "'}";
    }
  }

  /** A {@link FunctionChange} to add a new definition to a function. */
  final class AddDefinition implements FunctionChange {
    private final FunctionDefinition definition;

    AddDefinition(FunctionDefinition definition) {
      Preconditions.checkArgument(definition != null, "Definition cannot be null");
      this.definition = definition;
    }

    /**
     * @return The definition to add.
     */
    public FunctionDefinition definition() {
      return definition;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof AddDefinition)) {
        return false;
      }
      AddDefinition that = (AddDefinition) obj;
      return Objects.equals(definition, that.definition);
    }

    @Override
    public int hashCode() {
      return Objects.hash(definition);
    }

    @Override
    public String toString() {
      return "AddDefinition{definition=" + definition + '}';
    }
  }

  /** A {@link FunctionChange} to remove an existing definition from a function. */
  final class RemoveDefinition implements FunctionChange {
    private final FunctionParam[] parameters;

    RemoveDefinition(FunctionParam[] parameters) {
      Preconditions.checkArgument(parameters != null, "Parameters cannot be null");
      this.parameters = Arrays.copyOf(parameters, parameters.length);
    }

    /**
     * @return The parameters that identify the definition to remove.
     */
    public FunctionParam[] parameters() {
      return parameters.length == 0 ? EMPTY_PARAMS : Arrays.copyOf(parameters, parameters.length);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveDefinition)) {
        return false;
      }
      RemoveDefinition that = (RemoveDefinition) obj;
      return Arrays.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(parameters);
    }

    @Override
    public String toString() {
      return "RemoveDefinition{parameters=" + Arrays.toString(parameters) + '}';
    }
  }

  /** A {@link FunctionChange} to add an implementation to a definition. */
  final class AddImpl implements FunctionChange {
    private final FunctionParam[] parameters;
    private final FunctionImpl implementation;

    AddImpl(FunctionParam[] parameters, FunctionImpl implementation) {
      Preconditions.checkArgument(parameters != null, "Parameters cannot be null");
      this.parameters = Arrays.copyOf(parameters, parameters.length);
      Preconditions.checkArgument(implementation != null, "Implementation cannot be null");
      this.implementation = implementation;
    }

    /**
     * @return The parameters that identify the definition to update.
     */
    public FunctionParam[] parameters() {
      return parameters.length == 0 ? EMPTY_PARAMS : Arrays.copyOf(parameters, parameters.length);
    }

    /**
     * @return The implementation to add.
     */
    public FunctionImpl implementation() {
      return implementation;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof AddImpl)) {
        return false;
      }
      AddImpl that = (AddImpl) obj;
      return Arrays.equals(parameters, that.parameters)
          && Objects.equals(implementation, that.implementation);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(parameters);
      result = 31 * result + Objects.hashCode(implementation);
      return result;
    }

    @Override
    public String toString() {
      return "AddImpl{parameters="
          + Arrays.toString(parameters)
          + ", implementation="
          + implementation
          + '}';
    }
  }

  /**
   * A {@link FunctionChange} to replace an implementation (identified by runtime) for a specific
   * definition.
   */
  final class UpdateImpl implements FunctionChange {
    private final FunctionParam[] parameters;
    private final FunctionImpl.RuntimeType runtime;
    private final FunctionImpl implementation;

    UpdateImpl(
        FunctionParam[] parameters, FunctionImpl.RuntimeType runtime, FunctionImpl implementation) {
      Preconditions.checkArgument(parameters != null, "Parameters cannot be null");
      this.parameters = Arrays.copyOf(parameters, parameters.length);
      Preconditions.checkArgument(runtime != null, "Runtime cannot be null");
      this.runtime = runtime;
      Preconditions.checkArgument(implementation != null, "Implementation cannot be null");
      this.implementation = implementation;
      Preconditions.checkArgument(
          runtime == implementation.runtime(),
          "Runtime of implementation must match the runtime being updated");
    }

    /**
     * @return The parameters that identify the definition to update.
     */
    public FunctionParam[] parameters() {
      return parameters.length == 0 ? EMPTY_PARAMS : Arrays.copyOf(parameters, parameters.length);
    }

    /**
     * @return The runtime that identifies the implementation to replace.
     */
    public FunctionImpl.RuntimeType runtime() {
      return runtime;
    }

    /**
     * @return The new implementation.
     */
    public FunctionImpl implementation() {
      return implementation;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof UpdateImpl)) {
        return false;
      }
      UpdateImpl that = (UpdateImpl) obj;
      return Arrays.equals(parameters, that.parameters)
          && runtime == that.runtime
          && Objects.equals(implementation, that.implementation);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(parameters);
      result = 31 * result + Objects.hash(runtime, implementation);
      return result;
    }

    @Override
    public String toString() {
      return "UpdateImpl{parameters="
          + Arrays.toString(parameters)
          + ", runtime="
          + runtime
          + ", implementation="
          + implementation
          + '}';
    }
  }

  /** A {@link FunctionChange} to remove an implementation for a specific runtime. */
  final class RemoveImpl implements FunctionChange {
    private final FunctionParam[] parameters;
    private final FunctionImpl.RuntimeType runtime;

    RemoveImpl(FunctionParam[] parameters, FunctionImpl.RuntimeType runtime) {
      Preconditions.checkArgument(parameters != null, "Parameters cannot be null");
      this.parameters = Arrays.copyOf(parameters, parameters.length);
      Preconditions.checkArgument(runtime != null, "Runtime cannot be null");
      this.runtime = runtime;
    }

    /**
     * @return The parameters that identify the definition to update.
     */
    public FunctionParam[] parameters() {
      return parameters.length == 0 ? EMPTY_PARAMS : Arrays.copyOf(parameters, parameters.length);
    }

    /**
     * @return The runtime that identifies the implementation to remove.
     */
    public FunctionImpl.RuntimeType runtime() {
      return runtime;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveImpl)) {
        return false;
      }
      RemoveImpl that = (RemoveImpl) obj;
      return Arrays.equals(parameters, that.parameters) && runtime == that.runtime;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(parameters);
      result = 31 * result + Objects.hashCode(runtime);
      return result;
    }

    @Override
    public String toString() {
      return "RemoveImpl{parameters=" + Arrays.toString(parameters) + ", runtime=" + runtime + '}';
    }
  }
}
