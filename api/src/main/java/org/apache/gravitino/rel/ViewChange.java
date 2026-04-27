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
package org.apache.gravitino.rel;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/**
 * The ViewChange interface defines a change that can be applied to a view via {@link
 * ViewCatalog#alterView(org.apache.gravitino.NameIdentifier, ViewChange...)}.
 */
@Unstable
public interface ViewChange {

  /**
   * Create a ViewChange for renaming a view.
   *
   * @param newName The new view name.
   * @return A ViewChange for the rename.
   */
  static ViewChange rename(String newName) {
    return new RenameView(newName);
  }

  /**
   * Create a ViewChange for setting a view property.
   *
   * <p>If the property already exists, it will be replaced with the new value.
   *
   * @param property The property name.
   * @param value The new property value.
   * @return A ViewChange for the addition.
   */
  static ViewChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Create a ViewChange for removing a view property.
   *
   * <p>If the property does not exist, the change will succeed.
   *
   * @param property The property name.
   * @return A ViewChange for the removal.
   */
  static ViewChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Create a ViewChange that atomically replaces the view body, i.e. its columns, representations,
   * default catalog, default schema and comment. Properties and the view name are not affected by
   * this change.
   *
   * <p>The provided values are treated as a full replacement: any existing columns, representations
   * and defaults are discarded. Callers that only intend to change a subset of the body should read
   * the current values first and pass them back unchanged.
   *
   * @param columns The new output columns of the view.
   * @param representations The new representations of the view. At least one representation is
   *     expected.
   * @param defaultCatalog The new default catalog, or {@code null} to unset it.
   * @param defaultSchema The new default schema, or {@code null} to unset it.
   * @param comment The new comment, or {@code null} to unset it.
   * @return A ViewChange for the replacement.
   */
  static ViewChange replaceView(
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      @Nullable String comment) {
    return new ReplaceView(columns, representations, defaultCatalog, defaultSchema, comment);
  }

  /** A ViewChange to rename a view. */
  final class RenameView implements ViewChange {
    private final String newName;

    private RenameView(String newName) {
      Preconditions.checkArgument(
          newName != null && !newName.isEmpty(), "newName must not be null or empty");
      this.newName = newName;
    }

    /**
     * Retrieves the new name for the view.
     *
     * @return The new view name.
     */
    public String getNewName() {
      return newName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameView that = (RenameView) o;
      return newName.equals(that.newName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    @Override
    public String toString() {
      return "RENAMEVIEW " + newName;
    }
  }

  /**
   * A ViewChange to set a view property.
   *
   * <p>If the property already exists, it must be replaced with the new value.
   */
  final class SetProperty implements ViewChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(), "property must not be null or empty");
      this.property = property;
      this.value = value;
    }

    /**
     * Retrieves the name of the property.
     *
     * @return The property name.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Retrieves the value of the property.
     *
     * @return The property value.
     */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetProperty that = (SetProperty) o;
      return property.equals(that.property) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    @Override
    public String toString() {
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /**
   * A ViewChange to remove a view property.
   *
   * <p>If the property does not exist, the change should succeed.
   */
  final class RemoveProperty implements ViewChange {
    private final String property;

    private RemoveProperty(String property) {
      Preconditions.checkArgument(
          property != null && !property.isEmpty(), "property must not be null or empty");
      this.property = property;
    }

    /**
     * Retrieves the name of the property to be removed from the view.
     *
     * @return The property name scheduled for removal.
     */
    public String getProperty() {
      return property;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RemoveProperty that = (RemoveProperty) o;
      return property.equals(that.property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }

  /**
   * A ViewChange that atomically replaces the view body (columns, representations, default catalog,
   * default schema and comment).
   */
  final class ReplaceView implements ViewChange {
    private final Column[] columns;
    private final Representation[] representations;
    @Nullable private final String defaultCatalog;
    @Nullable private final String defaultSchema;
    @Nullable private final String comment;

    private ReplaceView(
        Column[] columns,
        Representation[] representations,
        @Nullable String defaultCatalog,
        @Nullable String defaultSchema,
        @Nullable String comment) {
      Preconditions.checkArgument(columns != null, "columns must not be null");
      Preconditions.checkArgument(
          representations != null && representations.length > 0,
          "representations must not be null or empty");
      this.columns = Arrays.copyOf(columns, columns.length);
      this.representations = Arrays.copyOf(representations, representations.length);
      this.defaultCatalog = defaultCatalog;
      this.defaultSchema = defaultSchema;
      this.comment = comment;
    }

    /**
     * Retrieves the new output columns of the view.
     *
     * @return The new columns.
     */
    public Column[] getColumns() {
      return Arrays.copyOf(columns, columns.length);
    }

    /**
     * Retrieves the new representations of the view.
     *
     * @return The new representations.
     */
    public Representation[] getRepresentations() {
      return Arrays.copyOf(representations, representations.length);
    }

    /**
     * Retrieves the new default catalog, or {@code null} if it should be unset.
     *
     * @return The new default catalog, or {@code null}.
     */
    @Nullable
    public String getDefaultCatalog() {
      return defaultCatalog;
    }

    /**
     * Retrieves the new default schema, or {@code null} if it should be unset.
     *
     * @return The new default schema, or {@code null}.
     */
    @Nullable
    public String getDefaultSchema() {
      return defaultSchema;
    }

    /**
     * Retrieves the new view comment, or {@code null} if it should be unset.
     *
     * @return The new comment, or {@code null}.
     */
    @Nullable
    public String getComment() {
      return comment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ReplaceView that = (ReplaceView) o;
      return Arrays.equals(columns, that.columns)
          && Arrays.equals(representations, that.representations)
          && Objects.equals(defaultCatalog, that.defaultCatalog)
          && Objects.equals(defaultSchema, that.defaultSchema)
          && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(defaultCatalog, defaultSchema, comment);
      result = 31 * result + Arrays.hashCode(columns);
      result = 31 * result + Arrays.hashCode(representations);
      return result;
    }

    @Override
    public String toString() {
      return "REPLACEVIEW columns="
          + Arrays.toString(columns)
          + ", representations="
          + Arrays.toString(representations)
          + ", defaultCatalog="
          + defaultCatalog
          + ", defaultSchema="
          + defaultSchema
          + ", comment="
          + comment;
    }
  }
}
