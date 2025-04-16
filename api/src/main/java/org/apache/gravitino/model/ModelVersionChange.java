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

package org.apache.gravitino.model;

import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/**
 * A model version change is a change to a model version. It can be used to update uri of a model
 * version, update the comment of a model, set a property and value pair for a model version, or
 * remove a property from a model.
 */
@Evolving
public interface ModelVersionChange {

  /**
   * Create a ModelVersionChange for updating the comment of a model version.
   *
   * @param newComment new comment to be set for the model version
   * @return a new ModelVersionChange instance for updating the comment of a model version
   */
  static ModelVersionChange updateComment(String newComment) {
    return new ModelVersionChange.UpdateComment(newComment);
  }

  /** A ModelVersionChange to update the modelve version comment. */
  final class UpdateComment implements ModelVersionChange {

    private final String newComment;

    /**
     * Creates a new {@link UpdateComment} instance with the specified new comment.
     *
     * @param newComment new comment to be set for the model version
     */
    public UpdateComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Returns the new comment to be set for the model version.
     *
     * @return the new comment to be set for the model version
     */
    public String newComment() {
      return newComment;
    }

    /**
     * Compares this {@link UpdateComment} instance with another object for equality. The comparison
     * is based on the new comment of the model version.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof UpdateComment)) return false;
      UpdateComment other = (UpdateComment) obj;
      return Objects.equals(newComment, other.newComment);
    }

    /**
     * Generates a hash code for this {@link UpdateComment} instance. The hash code is based on the
     * new comment of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Returns a string representation of the {@link UpdateComment} instance. This string format
     * includes the class name followed by the comment to be updated.
     *
     * @return A string summary of the {@link UpdateComment} instance.
     */
    @Override
    public String toString() {
      return "UpdateComment " + newComment;
    }
  }
}
