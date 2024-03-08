/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.messaging;

import com.datastrato.gravitino.annotation.Evolving;
import java.util.Objects;

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
}
