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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.annotation.Evolving;

/**
 * A model version change is a change to a model version. It can be used to update uri of a model
 * version, update the comment of a model, set a property and value pair for a model version, or
 * remove a property from a model.
 */
@Evolving
public interface ModelVersionChange {
  /** A Joiner for comma-separated values. */
  Joiner COMMA_JOINER = Joiner.on(",").skipNulls();

  /**
   * Create a ModelVersionChange for updating the comment of a model version.
   *
   * @param newComment new comment to be set for the model version
   * @return A new ModelVersionChange instance for updating the comment of a model version
   */
  static ModelVersionChange updateComment(String newComment) {
    return new ModelVersionChange.UpdateComment(newComment);
  }

  /**
   * Create a ModelVersionChange for setting a property of a model version.
   *
   * @param property name of the property to be set
   * @param value value to be set for the property
   * @return A new ModelVersionChange instance for setting a property of a model version
   */
  static ModelVersionChange setProperty(String property, String value) {
    return new ModelVersionChange.SetProperty(property, value);
  }

  /**
   * Create a ModelVersionChange for removing a property from a model version.
   *
   * @param property The name of the property to be removed.
   * @return The new ModelVersionChange instance for removing a property from a model version
   */
  static ModelVersionChange removeProperty(String property) {
    return new ModelVersionChange.RemoveProperty(property);
  }

  /**
   * Create a ModelVersionChange for updating the uri of a model version.
   *
   * @param newUri The new uri to be set for the model version.
   * @return A new ModelVersionChange instance for updating the uri of a model version.
   */
  static ModelVersionChange updateUri(String newUri) {
    return new ModelVersionChange.UpdateUri(newUri);
  }

  /**
   * Create a ModelVersionChange for updating the uri of a model version.
   *
   * @param uriName The name of the uri to be updated for the model version.
   * @param newUri The new uri to be set for the model version.
   * @return A new ModelVersionChange instance for updating the uri of a model version.
   */
  static ModelVersionChange updateUri(String uriName, String newUri) {
    return new ModelVersionChange.UpdateUri(uriName, newUri);
  }

  /**
   * Create a ModelVersionChange for adding the uri of a model version.
   *
   * @param uriName The name of the uri to be added for the model version.
   * @param uri The uri to be added for the model version.
   * @return A new ModelVersionChange instance for adding the uri of a model version.
   */
  static ModelVersionChange addUri(String uriName, String uri) {
    return new ModelVersionChange.AddUri(uriName, uri);
  }

  /**
   * Create a ModelVersionChange for removing the uri of a model version.
   *
   * @param uriName The name of the uri to be removed for the model version.
   * @return A new ModelVersionChange instance for removing the uri of a model version.
   */
  static ModelVersionChange removeUri(String uriName) {
    return new ModelVersionChange.RemoveUri(uriName);
  }

  /**
   * Create a ModelVersionChange for updating the aliases of a model version.
   *
   * @param aliasesToAdd The new aliases to be added for the model version.
   * @param aliasesToRemove The aliases to be removed from the model version.
   * @return A new ModelVersionChange instance for updating the aliases of a model version.
   */
  static ModelVersionChange updateAliases(String[] aliasesToAdd, String[] aliasesToRemove) {
    String[] toAdd = aliasesToAdd == null ? new String[0] : aliasesToAdd;
    String[] toRemove = aliasesToRemove == null ? new String[0] : aliasesToRemove;

    return new UpdateAliases(
        Arrays.stream(toAdd).collect(Collectors.toList()),
        Arrays.stream(toRemove).collect(Collectors.toList()));
  }

  /** A ModelVersionChange to update the model version comment. */
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
     * @return A hash code value for this comment update operation.
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

  /** A ModelVersionChange to set a property of a model version. */
  final class SetProperty implements ModelVersionChange {
    private final String property;
    private final String value;

    /**
     * Creates a new {@link SetProperty} instance with the specified property name and value.
     *
     * @param property name of the property to be set
     * @param value value to be set for the property
     */
    public SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Returns the name of the property to be set.
     *
     * @return the name of the property to be set
     */
    public String property() {
      return property;
    }

    /**
     * Returns the value to be set for the property.
     *
     * @return the value to be set for the property
     */
    public String value() {
      return value;
    }

    /**
     * Compares this SetProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property and set the same value.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same property set; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof ModelVersionChange.SetProperty)) return false;
      ModelVersionChange.SetProperty other = (ModelVersionChange.SetProperty) obj;
      return Objects.equals(property, other.property) && Objects.equals(value, other.value);
    }

    /**
     * Generates a hash code for this SetProperty instance. The hash code is based on the property
     * name and value to be set.
     *
     * @return A hash code value for this property set operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /**
     * Provides a string representation of the SetProperty instance. This string format includes the
     * class name followed by the property name and value to be set.
     *
     * @return A string summary of the property set operation.
     */
    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A ModelVersionChange to remove a property from a model version. */
  final class RemoveProperty implements ModelVersionChange {
    private final String property;

    /**
     * Creates a new {@link RemoveProperty} instance with the specified property name.
     *
     * @param property name of the property to be removed
     */
    public RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Returns the name of the property to be removed.
     *
     * @return the name of the property to be removed
     */
    public String property() {
      return property;
    }

    /**
     * Compares this RemoveProperty instance with another object for equality. Two instances are
     * considered equal if they target the same property.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same property removal; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof ModelVersionChange.RemoveProperty)) return false;
      ModelVersionChange.RemoveProperty other = (ModelVersionChange.RemoveProperty) obj;
      return Objects.equals(property, other.property);
    }

    /**
     * Generates a hash code for this RemoveProperty instance. The hash code is based on the
     * property name to be removed.
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

  /** A ModelVersionChange to update the uri of a model version. */
  final class UpdateUri implements ModelVersionChange {
    private final String uriName;
    private final String newUri;

    /**
     * Creates a new {@link UpdateUri} instance with the specified new uri.
     *
     * @param newUri The new uri to be set for the model version.
     */
    public UpdateUri(String newUri) {
      this(ModelVersion.URI_NAME_UNKNOWN, newUri);
    }

    /**
     * Creates a new {@link UpdateUri} instance with the specified new uri and its name.
     *
     * @param uriName The name of the uri to be updated for the model version.
     * @param newUri The new uri to be set for the model version.
     */
    public UpdateUri(String uriName, String newUri) {
      this.uriName = uriName;
      this.newUri = newUri;
    }

    /**
     * Returns the new uri to be set for the model version.
     *
     * @return The new uri to be set for the model version.
     */
    public String newUri() {
      return newUri;
    }

    /**
     * Returns the name of the uri to be updated for the model version.
     *
     * @return The name of the uri to be updated for the model version.
     */
    public String uriName() {
      return uriName;
    }

    /**
     * Compares this UpdateUri instance with another object for equality. The comparison is based on
     * the new uri and its name of the model version.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof UpdateUri)) return false;
      UpdateUri other = (UpdateUri) obj;
      return Objects.equals(newUri, other.newUri) && Objects.equals(uriName, other.uriName);
    }

    /**
     * Generates a hash code for this UpdateUri instance. The hash code is based on the new uri and
     * its name of the model version.
     *
     * @return A hash code value for this URI update operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(newUri, uriName);
    }

    /**
     * Provides a string representation of the UpdateUri instance. This string format includes the
     * class name followed by the new uri and its name to be updated.
     *
     * @return A string summary of the UpdateUri instance.
     */
    @Override
    public String toString() {
      return "UpdateUri uriName: (" + uriName + ") newUri: (" + newUri + ")";
    }
  }

  /** A ModelVersionChange to add a uri of a model version. */
  final class AddUri implements ModelVersionChange {
    private final String uriName;
    private final String uri;

    /**
     * Creates a new {@link AddUri} instance with the specified uri and uri name.
     *
     * @param uriName The name of the uri to be added.
     * @param uri The uri to be added for the model version.
     */
    public AddUri(String uriName, String uri) {
      this.uriName = uriName;
      this.uri = uri;
    }

    /**
     * Returns the uri to be added for the model version.
     *
     * @return The uri to be added for the model version.
     */
    public String uri() {
      return uri;
    }

    /**
     * Returns the name of the uri to be added for the model version.
     *
     * @return The name of the uri to be added for the model version.
     */
    public String uriName() {
      return uriName;
    }

    /**
     * Compares this AddUri instance with another object for equality. The comparison is based on
     * the uri and its name of the model version.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof AddUri)) return false;
      AddUri other = (AddUri) obj;
      return Objects.equals(uri, other.uri) && Objects.equals(uriName, other.uriName);
    }

    /**
     * Generates a hash code for this AddUri instance. The hash code is based on the uri and its
     * name of the model.
     *
     * @return A hash code value for this URI addition operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(uri, uriName);
    }

    /**
     * Provides a string representation of the AddUri instance. This string format includes the
     * class name followed by the uri and its name to be added.
     *
     * @return A string summary of the AddUri instance.
     */
    @Override
    public String toString() {
      return "AddUri uriName: (" + uriName + ") uri: (" + uri + ")";
    }
  }

  /** A ModelVersionChange to remove a uri of a model version. */
  final class RemoveUri implements ModelVersionChange {
    private final String uriName;

    /**
     * Creates a new {@link RemoveUri} instance with the specified uri name.
     *
     * @param uriName The name of the uri to be removed.
     */
    public RemoveUri(String uriName) {
      this.uriName = uriName;
    }

    /**
     * Returns the name of the uri to be removed for the model version.
     *
     * @return The name of the uri to be removed for the model version.
     */
    public String uriName() {
      return uriName;
    }

    /**
     * Compares this RemoveUri instance with another object for equality. The comparison is based on
     * the uri name of the model version.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (!(obj instanceof RemoveUri)) return false;
      RemoveUri other = (RemoveUri) obj;
      return Objects.equals(uriName, other.uriName);
    }

    /**
     * Generates a hash code for this RemoveUri instance. The hash code is based on the uri and its
     * name of the model.
     *
     * @return A hash code value for this URI removal operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(uriName);
    }

    /**
     * Provides a string representation of the RemoveUri instance. This string format includes the
     * class name followed by the uri name to be removed.
     *
     * @return A string summary of the RemoveUri instance.
     */
    @Override
    public String toString() {
      return "RemoveUri uriName: (" + uriName + ")";
    }
  }

  /**
   * Represents an update to a model version’s aliases, specifying which aliases to add and which to
   * remove.
   *
   * <p>Both alias sets are stored as immutable.
   */
  final class UpdateAliases implements ModelVersionChange {
    private final ImmutableSortedSet<String> aliasesToAdd;
    private final ImmutableSortedSet<String> aliasesToRemove;

    /**
     * Constructs a new aliases-update operation, specifying the aliases to add and remove.
     *
     * @param aliasesToAdd the aliases to add, or null for none
     * @param aliasesToRemove the aliases to remove, or null for none
     */
    public UpdateAliases(List<String> aliasesToAdd, List<String> aliasesToRemove) {
      this.aliasesToAdd =
          ImmutableSortedSet.copyOf(aliasesToAdd != null ? aliasesToAdd : Lists.newArrayList());
      this.aliasesToRemove =
          ImmutableSortedSet.copyOf(
              aliasesToRemove != null ? aliasesToRemove : Lists.newArrayList());
    }

    /**
     * Returns the set of aliases to add.
     *
     * @return an immutable, sorted set of aliases to add
     */
    public Set<String> aliasesToAdd() {
      return aliasesToAdd;
    }

    /**
     * Returns the set of aliases to remove.
     *
     * @return an immutable, sorted set of aliases to remove
     */
    public Set<String> aliasesToRemove() {
      return aliasesToRemove;
    }

    /**
     * Compares this UpdateAlias instance with another object for equality. The comparison is based
     * on the both new and removed aliases of the model version.
     *
     * @param o The object to compare with this instance.
     * @return {@code true} if the given object represents the same model update operation; {@code
     *     false} otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof UpdateAliases)) return false;
      UpdateAliases that = (UpdateAliases) o;
      return aliasesToAdd.equals(that.aliasesToAdd) && aliasesToRemove.equals(that.aliasesToRemove);
    }

    /**
     * Generates a hash code for this UpdateAlias instance. The hash code is based on the both new
     * and removed aliases of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(aliasesToAdd, aliasesToRemove);
    }

    /**
     * Provides a string representation of the UpdateAlias instance. This string format includes the
     * class name followed by the new and removed aliases to be set and removed.
     *
     * @return A string summary of the UpdateAlias instance.
     */
    @Override
    public String toString() {
      return "UpdateAlias "
          + "AliasToAdd: ("
          + COMMA_JOINER.join(aliasesToAdd)
          + ")"
          + " "
          + "AliasToRemove: ("
          + COMMA_JOINER.join(aliasesToRemove)
          + ")";
    }
  }
}
