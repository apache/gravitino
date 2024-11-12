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
package org.apache.gravitino.authorization;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;

/**
 * The MetadataObjectChange interface defines the public API for managing roles in an authorization.
 */
@Evolving
public interface MetadataObjectChange {
  /**
   * Rename a metadata entity MetadataObjectChange.
   *
   * @param metadataObject The metadata object.
   * @param newMetadataObject The new metadata object.
   * @return return a MetadataObjectChange for the rename metadata object.
   */
  static MetadataObjectChange rename(
      MetadataObject metadataObject, MetadataObject newMetadataObject) {
    return new RenameMetadataObject(metadataObject, newMetadataObject);
  }

  /**
   * Remove a metadata entity MetadataObjectChange.
   *
   * @param metadataObject The metadata object.
   * @return return a MetadataObjectChange for the remove metadata object.
   */
  static MetadataObjectChange remove(MetadataObject metadataObject) {
    return new RemoveMetadataObject(metadataObject);
  }

  /** A RenameMetadataObject is to rename securable object's metadata entity. */
  final class RenameMetadataObject implements MetadataObjectChange {
    private final MetadataObject metadataObject;
    private final MetadataObject newMetadataObject;

    private RenameMetadataObject(MetadataObject metadataObject, MetadataObject newMetadataObject) {
      Preconditions.checkArgument(
          !metadataObject.fullName().equals(newMetadataObject.fullName()),
          "The metadata object must be different!");
      Preconditions.checkArgument(
          metadataObject.type().equals(newMetadataObject.type()),
          "The metadata object type must be same!");

      this.metadataObject = metadataObject;
      this.newMetadataObject = newMetadataObject;
    }

    /**
     * Returns the metadataObject to be renamed.
     *
     * @return return a metadataObject.
     */
    public MetadataObject metadataObject() {
      return metadataObject;
    }

    /**
     * Returns the new metadataObject object.
     *
     * @return return a metadataObject object.
     */
    public MetadataObject newMetadataObject() {
      return newMetadataObject;
    }

    /**
     * Compares this RenameMetadataObject instance with another object for equality. The comparison
     * is based on the old metadata entity and new metadata entity.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same rename metadata entity; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameMetadataObject that = (RenameMetadataObject) o;
      return metadataObject.equals(that.metadataObject)
          && newMetadataObject.equals(that.newMetadataObject);
    }

    /**
     * Generates a hash code for this RenameMetadataObject instance. The hash code is based on the
     * old metadata entity and new metadata entity.
     *
     * @return A hash code value for this update metadata entity operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(metadataObject, newMetadataObject);
    }

    /**
     * Returns a string representation of the RenameMetadataObject instance. This string format
     * includes the class name followed by the update metadata entity object operation.
     *
     * @return A string representation of the RenameMetadataObject instance.
     */
    @Override
    public String toString() {
      return "RENAMEMETADATAOBJECT " + metadataObject + " " + newMetadataObject;
    }
  }

  /** A RemoveMetadataObject is to remove securable object's metadata entity. */
  final class RemoveMetadataObject implements MetadataObjectChange {
    private final MetadataObject metadataObject;

    private RemoveMetadataObject(MetadataObject metadataObject) {
      this.metadataObject = metadataObject;
    }

    /**
     * Returns the metadataObject to be renamed.
     *
     * @return return a metadataObject.
     */
    public MetadataObject metadataObject() {
      return metadataObject;
    }

    /**
     * Compares this RemoveMetadataObject instance with another object for equality. The comparison
     * is based on the old metadata entity.
     *
     * @param o The object to compare with this instance.
     * @return true if the given object represents the same rename metadata entity; false otherwise.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RenameMetadataObject that = (RenameMetadataObject) o;
      return metadataObject.equals(that.metadataObject);
    }

    /**
     * Generates a hash code for this RemoveMetadataObject instance. The hash code is based on the
     * old metadata entity.
     *
     * @return A hash code value for this update metadata entity operation.
     */
    @Override
    public int hashCode() {
      return Objects.hash(metadataObject);
    }

    /**
     * Returns a string representation of the RemoveMetadataObject instance. This string format
     * includes the class name followed by the remove metadata entity object operation.
     *
     * @return A string representation of the RemoveMetadataObject instance.
     */
    @Override
    public String toString() {
      return "REMOVEMETADATAOBJECT " + metadataObject;
    }
  }
}
