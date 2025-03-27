package org.apache.gravitino.model;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.annotation.Evolving;

/**
 * A model change is a change to a model. It can be used to rename a model, update the comment of a
 * model, set a property and value pair for a model, or remove a property from a model.
 */
@Evolving
public interface ModelChange {
  /**
   * Create a ModelChange for renaming a model.
   *
   * @param newName The new model name.
   * @return A ModelChange for the rename.
   */
  static ModelChange rename(String newName) {
    return new ModelChange.RenameModel(newName);
  }

  /**
   * Create a unsupported ModelChange for testing.
   *
   * @param message the error message.
   * @return A ModelChange for the unsupported operation.
   */
  static ModelChange unSupport(String message) {
    return new UnsupportModelChange(message);
  }

  /** A ModelChange to rename a model. */
  final class RenameModel implements ModelChange {
    private final String newName;

    /**
     * Constructs a new {@link RenameModel} instance with the specified new name.
     *
     * @param newName The new name of the model.
     */
    public RenameModel(String newName) {
      this.newName = newName;
    }

    /**
     * Retrieves the new name for the model.
     *
     * @return The new name of the model.
     */
    public String newName() {
      return newName;
    }

    /**
     * Compares this {@code RenameModel} instance with another object for equality. The comparison
     * is based on the new name of the model.
     *
     * @param obj The object to compare with this instance.
     * @return {@code true} if the given object represents the same model renaming; {@code false}
     *     otherwise.
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      RenameModel other = (RenameModel) obj;
      return newName.equals(other.newName);
    }

    /**
     * Generates a hash code for this {@code RenameModel} instance. The hash code is based on the
     * new name of the model.
     *
     * @return A hash code value for this model renaming operation.
     */
    @Override
    public int hashCode() {
      return newName.hashCode();
    }

    /**
     * Returns a string representation of the {@code RenameModel} instance. This string format
     * includes the class name followed by the property name to be renamed.
     *
     * @return A string summary of the property rename instance.
     */
    @Override
    public String toString() {
      return "RenameModel " + newName;
    }
  }

  /** A ModelChange to unsupport a model. */
  @VisibleForTesting
  final class UnsupportModelChange implements ModelChange {
    private final String message;

    /**
     * Constructs a new {@link UnsupportModelChange} instance with the specified error message.
     *
     * @param message The error message.
     */
    public UnsupportModelChange(String message) {
      this.message = message;
    }

    /**
     * Retrieves the error message.
     *
     * @return The error message.
     */
    public String message() {
      return message;
    }
  }
}
