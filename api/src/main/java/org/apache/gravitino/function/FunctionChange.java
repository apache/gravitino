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
import org.apache.gravitino.annotation.Evolving;

/** Represents a change that can be applied to a function. */
@Evolving
public interface FunctionChange {

  /**
   * Create a {@link FunctionChange} to update the comment of a function.
   *
   * @param newComment The new comment value.
   * @return The change instance.
   */
  static FunctionChange updateComment(String newComment) {
    return new UpdateComment(null, newComment);
  }

  /**
   * Create a {@link FunctionChange} to update the comment of a function.
   *
   * @param signature The function signature to match, null if only one function exists under the
   *     name.
   * @param newComment The new comment value.
   * @return The change instance.
   */
  static FunctionChange updateComment(FunctionSignature signature, String newComment) {
    return new UpdateComment(signature, newComment);
  }

  /**
   * Create a {@link FunctionChange} to replace all implementations of a function.
   *
   * @param newImplementations The new implementations.
   * @return The change instance.
   */
  static FunctionChange updateImplementations(FunctionImpl[] newImplementations) {
    return new UpdateImplementations(null, newImplementations);
  }

  /**
   * Create a {@link FunctionChange} to replace all implementations of a function.
   *
   * @param signature The function signature to match, null if only one function exists under the
   *     name.
   * @param newImplementations The new implementations.
   * @return The change instance.
   */
  static FunctionChange updateImplementations(
      FunctionSignature signature, FunctionImpl[] newImplementations) {
    return new UpdateImplementations(signature, newImplementations);
  }

  /**
   * Create a {@link FunctionChange} to add an implementation to a function.
   *
   * @param implementation The implementation to add.
   * @return The change instance.
   */
  static FunctionChange addImplementation(FunctionImpl implementation) {
    return new AddImplementation(null, implementation);
  }

  /**
   * Create a {@link FunctionChange} to add an implementation to a function.
   *
   * @param signature The function signature to match, null if only one function exists under the
   *     name.
   * @param implementation The implementation to add.
   * @return The change instance.
   */
  static FunctionChange addImplementation(
      FunctionSignature signature, FunctionImpl implementation) {
    return new AddImplementation(signature, implementation);
  }

  /**
   * Optional signature to disambiguate overloaded functions when applying the change.
   *
   * @return The optional function signature.
   */
  default FunctionSignature signature() {
    return null;
  }

  /** A {@link FunctionChange} to update the comment of a function. */
  final class UpdateComment implements FunctionChange {
    private final FunctionSignature signature;
    private final String newComment;

    UpdateComment(FunctionSignature signature, String newComment) {
      this.signature = signature;
      this.newComment = newComment;
    }

    /**
     * @return The function signature to match, null if only one function exists under the name.
     */
    @Override
    public FunctionSignature signature() {
      return signature;
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
      return Objects.equals(signature, that.signature)
          && Objects.equals(newComment, that.newComment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(signature, newComment);
    }

    @Override
    public String toString() {
      return "UpdateComment{signature=" + signature + ", newComment='" + newComment + "'}";
    }
  }

  /** A {@link FunctionChange} to replace all implementations of a function. */
  final class UpdateImplementations implements FunctionChange {
    private final FunctionSignature signature;
    private final FunctionImpl[] newImplementations;

    UpdateImplementations(FunctionSignature signature, FunctionImpl[] newImplementations) {
      Preconditions.checkArgument(
          newImplementations != null && newImplementations.length > 0,
          "Implementations cannot be null or empty");
      this.signature = signature;
      this.newImplementations = Arrays.copyOf(newImplementations, newImplementations.length);
    }

    /**
     * @return The function signature to match, null if only one function exists under the name.
     */
    @Override
    public FunctionSignature signature() {
      return signature;
    }

    /**
     * @return The new implementations of the function.
     */
    public FunctionImpl[] newImplementations() {
      return Arrays.copyOf(newImplementations, newImplementations.length);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof UpdateImplementations)) {
        return false;
      }
      UpdateImplementations that = (UpdateImplementations) obj;
      return Objects.equals(signature, that.signature)
          && Arrays.equals(newImplementations, that.newImplementations);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(signature);
      result = 31 * result + Arrays.hashCode(newImplementations);
      return result;
    }

    @Override
    public String toString() {
      return "UpdateImplementations{"
          + "signature="
          + signature
          + ", newImplementations="
          + Arrays.toString(newImplementations)
          + '}';
    }
  }

  /** A {@link FunctionChange} to add an implementation to a function. */
  final class AddImplementation implements FunctionChange {
    private final FunctionSignature signature;
    private final FunctionImpl implementation;

    AddImplementation(FunctionSignature signature, FunctionImpl implementation) {
      this.signature = signature;
      this.implementation =
          Preconditions.checkNotNull(implementation, "Implementation cannot be null");
    }

    /**
     * @return The function signature to match, null if only one function exists under the name.
     */
    @Override
    public FunctionSignature signature() {
      return signature;
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
      if (!(obj instanceof AddImplementation)) {
        return false;
      }
      AddImplementation that = (AddImplementation) obj;
      return Objects.equals(signature, that.signature)
          && Objects.equals(implementation, that.implementation);
    }

    @Override
    public int hashCode() {
      return Objects.hash(signature, implementation);
    }

    @Override
    public String toString() {
      return "AddImplementation{signature="
          + signature
          + ", implementation="
          + implementation
          + '}';
    }
  }
}
