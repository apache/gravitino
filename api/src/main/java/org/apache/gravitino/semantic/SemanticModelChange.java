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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * A change that can be applied to a Semantic Model through {@link
 * SemanticModelCatalog#alterSemanticModel(org.apache.gravitino.NameIdentifier,
 * SemanticModelChange...)}.
 */
@Evolving
public interface SemanticModelChange {

  /**
   * Creates a change that renames a Semantic Model.
   *
   * @param newName The new Semantic Model name.
   * @return The rename change.
   */
  static SemanticModelChange rename(String newName) {
    return new RenameSemanticModel(newName);
  }

  /**
   * Creates a change that replaces a Semantic Model comment.
   *
   * @param newComment The new comment, or {@code null} to remove the current comment.
   * @return The comment change.
   */
  static SemanticModelChange updateComment(@Nullable String newComment) {
    return new UpdateComment(newComment);
  }

  /**
   * Creates a change that sets a Gravitino-specific Semantic Model property.
   *
   * <p>If the property already exists, its value is replaced.
   *
   * @param property The property name.
   * @param value The property value.
   * @return The property change.
   */
  static SemanticModelChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a change that removes a Gravitino-specific Semantic Model property.
   *
   * <p>If the property does not exist, applying the change succeeds without modifying the model.
   *
   * @param property The property name.
   * @return The property removal change.
   */
  static SemanticModelChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /**
   * Creates a change that atomically replaces the complete Semantic Model definition.
   *
   * <p>The Semantic Model name, comment, and Gravitino-specific properties are not affected.
   *
   * @param definition The replacement definition.
   * @return The definition replacement change.
   */
  static SemanticModelChange replaceDefinition(SemanticModelDefinition definition) {
    return new ReplaceDefinition(definition);
  }

  /** A change that renames a Semantic Model. */
  final class RenameSemanticModel implements SemanticModelChange {
    private final String newName;

    private RenameSemanticModel(String newName) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "New name must not be null or blank");
      this.newName = newName;
    }

    /**
     * Returns the new Semantic Model name.
     *
     * @return The new name.
     */
    public String getNewName() {
      return newName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RenameSemanticModel)) {
        return false;
      }
      RenameSemanticModel that = (RenameSemanticModel) obj;
      return newName.equals(that.newName);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(newName);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "RenameSemanticModel{newName='" + newName + "'}";
    }
  }

  /** A change that replaces a Semantic Model comment. */
  final class UpdateComment implements SemanticModelChange {
    @Nullable private final String newComment;

    private UpdateComment(@Nullable String newComment) {
      this.newComment = newComment;
    }

    /**
     * Returns the new Semantic Model comment.
     *
     * @return The new comment, or {@code null} if the current comment should be removed.
     */
    @Nullable
    public String getNewComment() {
      return newComment;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "UpdateComment{newComment='" + newComment + "'}";
    }
  }

  /** A change that sets a Gravitino-specific Semantic Model property. */
  final class SetProperty implements SemanticModelChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "Property name must not be null or blank");
      Preconditions.checkArgument(value != null, "Property value must not be null");
      this.property = property;
      this.value = value;
    }

    /**
     * Returns the property name.
     *
     * @return The property name.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Returns the property value.
     *
     * @return The property value.
     */
    public String getValue() {
      return value;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof SetProperty)) {
        return false;
      }
      SetProperty that = (SetProperty) obj;
      return property.equals(that.property) && value.equals(that.value);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "SetProperty{property='" + property + "', value='" + value + "'}";
    }
  }

  /** A change that removes a Gravitino-specific Semantic Model property. */
  final class RemoveProperty implements SemanticModelChange {
    private final String property;

    private RemoveProperty(String property) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "Property name must not be null or blank");
      this.property = property;
    }

    /**
     * Returns the property name.
     *
     * @return The property name.
     */
    public String getProperty() {
      return property;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof RemoveProperty)) {
        return false;
      }
      RemoveProperty that = (RemoveProperty) obj;
      return property.equals(that.property);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "RemoveProperty{property='" + property + "'}";
    }
  }

  /** A change that atomically replaces the complete Semantic Model definition. */
  final class ReplaceDefinition implements SemanticModelChange {
    private final SemanticModelDefinition definition;

    private ReplaceDefinition(SemanticModelDefinition definition) {
      Preconditions.checkArgument(definition != null, "Definition must not be null");
      this.definition = definition;
    }

    /**
     * Returns the immutable replacement definition.
     *
     * @return The replacement definition.
     */
    public SemanticModelDefinition getDefinition() {
      return definition;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ReplaceDefinition)) {
        return false;
      }
      ReplaceDefinition that = (ReplaceDefinition) obj;
      return definition.equals(that.definition);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hash(definition);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "ReplaceDefinition{definition=" + definition + "}";
    }
  }
}
