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
 * A model change is a change to a model. It can be used to rename a model, update the comment of a
 * model, set a property and value pair for a model, or remove a property from a model.
 */
@Evolving
public interface ModelChange {

  /**
   * Creates a new model change to update the model comment.
   *
   * @param newComment The new comment for the model.
   * @return The model change.
   */
  static ModelChange updateComment(String newComment) {
    return new UpdateModelComment(newComment);
  }

  /** A model change to update the model comment. */
  final class UpdateModelComment implements ModelChange {
    private final String newComment;

    private UpdateModelComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment intended for the model.
     *
     * @return The new comment that has been set for the model.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateModelComment instance with another object for equality. Two instances are
     * considered equal if they designate the same new comment for the model.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UpdateModelComment that = (UpdateModelComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateFileComment instance. The hash code is based on the new
     * comment for the model.
     *
     * @return A hash code representing this comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateModelComment instance. This string format
     * includes the class name followed by the new comment for the model.
     *
     * @return A string summary of this comment update operation.
     */
    @Override
    public String toString() {
      return "UPDATEMODELCOMMENT " + newComment;
    }
  }
}
