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
package org.apache.gravitino.file;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.FilesetChange.RenameFileset;

/**
 * The FilesetCatalog interface defines the public API for managing fileset objects in a schema. If
 * the catalog implementation supports fileset objects, it should implement this interface.
 */
@Evolving
public interface FilesetCatalog {

  /**
   * List the filesets in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of fileset identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load fileset metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A fileset identifier.
   * @return The fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException;

  /**
   * Check if a fileset exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A fileset identifier.
   * @return true If the fileset exists, false otherwise.
   */
  default boolean filesetExists(NameIdentifier ident) {
    try {
      loadFileset(ident);
      return true;
    } catch (NoSuchFilesetException e) {
      return false;
    }
  }

  /**
   * Create a fileset metadata with a default location in the catalog.
   *
   * <p>If the type of the fileset object is "MANAGED", the underlying storageLocation can be null,
   * and Gravitino will manage the storage location based on the location of the schema.
   *
   * <p>If the type of the fileset object is "EXTERNAL", the underlying storageLocation must be set.
   *
   * @param ident A fileset identifier.
   * @param comment The comment of the fileset.
   * @param type The type of the fileset.
   * @param storageLocation The storage location of the fileset.
   * @param properties The properties of the fileset.
   * @return The created fileset metadata
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FilesetAlreadyExistsException If the fileset already exists.
   */
  default Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    return createMultipleLocationFileset(
        ident,
        comment,
        type,
        storageLocation == null
            ? ImmutableMap.of()
            : ImmutableMap.of(LOCATION_NAME_UNKNOWN, storageLocation),
        properties);
  }

  /**
   * Create a fileset metadata with multiple storage locations in the catalog.
   *
   * @param ident A fileset identifier.
   * @param comment The comment of the fileset.
   * @param type The type of the fileset.
   * @param storageLocations The location names and storage locations of the fileset.
   * @param properties The properties of the fileset.
   * @return The created fileset metadata
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FilesetAlreadyExistsException If the fileset already exists.
   */
  default Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Apply the {@link FilesetChange change} to a fileset in the catalog.
   *
   * <p>Implementation may reject the change. If any change is rejected, no changes should be
   * applied to the fileset.
   *
   * <p>The {@link RenameFileset} change will only update the fileset name, the underlying storage
   * location for managed fileset will not be renamed.
   *
   * @param ident A fileset identifier.
   * @param changes The changes to apply to the fileset.
   * @return The altered fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException;

  /**
   * Drop a fileset from the catalog.
   *
   * <p>The underlying files will be deleted if this fileset type is managed, otherwise, only the
   * metadata will be dropped.
   *
   * @param ident A fileset identifier.
   * @return true If the fileset is dropped, false the fileset did not exist.
   */
  boolean dropFileset(NameIdentifier ident);

  /**
   * Get the actual location of a file or directory based on the default storage location of Fileset
   * and the sub path.
   *
   * @param ident A fileset identifier.
   * @param subPath The sub path to the file or directory.
   * @return The actual location of the file or directory.
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  default String getFileLocation(NameIdentifier ident, String subPath)
      throws NoSuchFilesetException {
    return getFileLocation(ident, subPath, null);
  }

  /**
   * Get the actual location of a file or directory based on the storage location of Fileset and the
   * sub path by the location name.
   *
   * @param ident A fileset identifier.
   * @param subPath The sub path to the file or directory.
   * @param locationName The location name. If null, the default location will be used.
   * @return The actual location of the file or directory.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws NoSuchLocationNameException If the location name does not exist.
   */
  default String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
