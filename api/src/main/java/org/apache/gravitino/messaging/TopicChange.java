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
package org.apache.gravitino.messaging;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/**
 * A topic change is a change to a topic. It can be used to update the comment of a topic, set a
 * property and value pair for a topic, or remove a property from a topic in the catalog.
 */
@Evolving
public interface TopicChange {

  /**
   * Creates a new topic change to update the topic comment.
   *
   * @param newComment The new comment for the topic.
   * @return The topic change.
   */
  static TopicChange updateComment(String newComment) {
    return new TopicChange.UpdateTopicComment(newComment);
  }

  /**
   * Creates a new topic change to set or update the property and value for the topic.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The topic change.
   */
  static TopicChange setProperty(String property, String value) {
    return new TopicChange.SetProperty(property, value);
  }

  /**
   * Creates a new topic change to remove a property from the topic.
   *
   * @param property The property name to remove.
   * @return The topic change.
   */
  static TopicChange removeProperty(String property) {
    return new TopicChange.RemoveProperty(property);
  }

  /**
   * Creates a new topic change to set or replace a named {@link DataLayout}.
   *
   * <p>Conventional names are {@link DataLayouts#KEY} and {@link DataLayouts#VALUE}.
   *
   * @param name The layout name. Must not be blank.
   * @param newDataLayout The new data layout. Must not be null.
   * @return The topic change.
   */
  static TopicChange updateDataLayout(String name, DataLayout newDataLayout) {
    return new TopicChange.UpdateDataLayout(name, newDataLayout);
  }

  /**
   * Creates a new topic change to set or replace the {@link DataLayouts#VALUE} layout.
   *
   * @param newDataLayout The new value layout. Must not be null.
   * @return The topic change.
   */
  static TopicChange updateValueDataLayout(DataLayout newDataLayout) {
    return updateDataLayout(DataLayouts.VALUE, newDataLayout);
  }

  /**
   * Creates a new topic change to remove a named {@link DataLayout}.
   *
   * @param name The layout name to remove. Must not be blank.
   * @return The topic change.
   */
  static TopicChange removeDataLayout(String name) {
    return new TopicChange.RemoveDataLayout(name);
  }

  /**
   * Creates a new topic change to clear all named {@link DataLayout}s.
   *
   * @return The topic change.
   */
  static TopicChange removeDataLayouts() {
    return new TopicChange.RemoveDataLayouts();
  }

  /** A topic change to update the topic comment. */
  final class UpdateTopicComment implements TopicChange {
    private final String newComment;

    private UpdateTopicComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Retrieves the new comment for the topic.
     *
     * @return The new comment for the topic.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateTopicComment instance with another object for equality. Two instances are
     * considered equal if they have the same new comment for the topic.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same comment update; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TopicChange.UpdateTopicComment that = (TopicChange.UpdateTopicComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateTopicComment instance. The hash code is based on the new
     * comment for the topic.
     *
     * @return A hash code representing this comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /**
     * Provides a string representation of the UpdateTopicComment instance. This string format
     * includes the class name followed by the new comment for the topic.
     *
     * @return A string summary of this comment update operation.
     */
    @Override
    public String toString() {
      return "UPDATETOPICCOMMENT " + newComment;
    }
  }

  /** A topic change to set or update the property and value for the topic. */
  final class SetProperty implements TopicChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property being set in the topic.
     *
     * @return The name of the property.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value assigned to the property in the topic.
     *
     * @return The value of the property.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they have the same property and value for the topic.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on both the
     * property name and its assigned value.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property and its value.
     *
     * @return A string summary of the property setting.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A topic change to remove a property from the topic. */
  final class RemoveProperty implements TopicChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the topic.
     *
     * @return The name of the property for removal.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property for removal from the topic.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name that is to be removed from the topic.
     *
     * @return A hash code value for this property removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /**
     * Provides a string representation of the RemoveProperty instance. This string format includes
     * the class name followed by the property name to be removed.
     *
     * @return A string summary of the property removal operation.
     */
    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }

  /** A topic change to update a named {@link DataLayout}. */
  final class UpdateDataLayout implements TopicChange {
    private final String name;
    private final DataLayout newDataLayout;

    private UpdateDataLayout(String name, DataLayout newDataLayout) {
      Preconditions.checkArgument(
          name != null && !name.trim().isEmpty(), "layout name must not be blank");
      this.name = name;
      this.newDataLayout = Objects.requireNonNull(newDataLayout, "newDataLayout");
    }

    /**
     * @return The layout name being updated.
     */
    public String getName() {
      return name;
    }

    /**
     * @return The new data layout.
     */
    public DataLayout getNewDataLayout() {
      return newDataLayout;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateDataLayout that = (UpdateDataLayout) o;
      return Objects.equals(name, that.name) && Objects.equals(newDataLayout, that.newDataLayout);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, newDataLayout);
    }

    @Override
    public String toString() {
      return "UPDATEDATALAYOUT " + name + " " + newDataLayout;
    }
  }

  /** A topic change to remove a named {@link DataLayout}. */
  final class RemoveDataLayout implements TopicChange {
    private final String name;

    private RemoveDataLayout(String name) {
      Preconditions.checkArgument(
          name != null && !name.trim().isEmpty(), "layout name must not be blank");
      this.name = name;
    }

    /**
     * @return The layout name being removed.
     */
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RemoveDataLayout)) {
        return false;
      }
      RemoveDataLayout that = (RemoveDataLayout) o;
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }

    @Override
    public String toString() {
      return "REMOVEDATALAYOUT " + name;
    }
  }

  /** A topic change to remove all named {@link DataLayout}s. */
  final class RemoveDataLayouts implements TopicChange {
    private RemoveDataLayouts() {}

    @Override
    public boolean equals(Object o) {
      return o instanceof RemoveDataLayouts;
    }

    @Override
    public int hashCode() {
      return RemoveDataLayouts.class.hashCode();
    }

    @Override
    public String toString() {
      return "REMOVEDATALAYOUTS";
    }
  }
}
