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
package org.apache.gravitino.domain;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * Interface for supporting domain changes. This interface will be used to provide domain
 * modification operations for each domain implementation.
 */
@Evolving
public interface DomainChange {

  /**
   * Creates a new domain change to rename the domain. The backing tag of the domain is not renamed.
   *
   * @param newName The new name of the domain.
   * @return The domain change.
   */
  static DomainChange rename(String newName) {
    return new RenameDomain(newName);
  }

  /**
   * Creates a new domain change to update the domain comment.
   *
   * @param newComment The new comment for the domain.
   * @return The domain change.
   */
  static DomainChange updateComment(String newComment) {
    return new UpdateDomainComment(newComment);
  }

  /**
   * Creates a new domain change to set the property and value for the domain.
   *
   * @param property The property name to set.
   * @param value The value to set the property to.
   * @return The domain change.
   */
  static DomainChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a new domain change to remove a property from the domain.
   *
   * @param property The property name to remove.
   * @return The domain change.
   */
  static DomainChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A domain change to rename the domain. */
  final class RenameDomain implements DomainChange {
    private final String newName;

    private RenameDomain(String newName) {
      this.newName = newName;
    }

    /**
     * Get the new name of the domain.
     *
     * @return The new name of the domain.
     */
    public String getNewName() {
      return newName;
    }

    /**
     * Compares this RenameDomain instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same domain rename operation; false
     *     otherwise.
     */
    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RenameDomain that = (RenameDomain) o;
      return Objects.equals(newName, that.newName);
    }

    /**
     * Generates a hash code for this RenameDomain instance.
     *
     * @return A hash code value for this domain rename operation.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(newName);
    }

    /**
     * Returns a string representation of the RenameDomain instance.
     *
     * @return A string representation of the RenameDomain instance.
     */
    @Override
    public String toString() {
      return "RENAMEDOMAIN " + newName;
    }
  }

  /** A domain change to update the domain comment. */
  final class UpdateDomainComment implements DomainChange {
    private final String newComment;

    private UpdateDomainComment(String newComment) {
      this.newComment = newComment;
    }

    /**
     * Get the new comment of the domain.
     *
     * @return The new comment of the domain.
     */
    public String getNewComment() {
      return newComment;
    }

    /**
     * Compares this UpdateDomainComment instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same domain comment update operation; false
     *     otherwise.
     */
    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateDomainComment that = (UpdateDomainComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    /**
     * Generates a hash code for this UpdateDomainComment instance.
     *
     * @return A hash code value for this domain comment update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(newComment);
    }

    /**
     * Returns a string representation of the UpdateDomainComment instance.
     *
     * @return A string representation of the UpdateDomainComment instance.
     */
    @Override
    public String toString() {
      return "UPDATEDOMAINCOMMENT " + newComment;
    }
  }

  /** A domain change to set the property and value for the domain. */
  final class SetProperty implements DomainChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Get the property name to set.
     *
     * @return The property name to set.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Get the value to set the property to.
     *
     * @return The value to set the property to.
     */
    public String getValue() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property setting; false otherwise.
     */
    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    /**
     * Generates a hash code for this SetProperty instance.
     *
     * @return A hash code value for this property setting.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Returns a string representation of the SetProperty instance.
     *
     * @return A string representation of the SetProperty instance.
     */
    @Override
    public String toString() {
      return "SETDOMAINPROPERTY " + property + " = " + value;
    }
  }

  /** A domain change to remove a property from the domain. */
  final class RemoveProperty implements DomainChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Get the property name to remove.
     *
     * @return The property name to remove.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same property removal; false otherwise.
     */
    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance.
     *
     * @return A hash code value for this property removal.
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(property);
    }

    /**
     * Returns a string representation of the RemoveProperty instance.
     *
     * @return A string representation of the RemoveProperty instance.
     */
    @Override
    public String toString() {
      return "REMOVEDOMAINPROPERTY " + property;
    }
  }
}
