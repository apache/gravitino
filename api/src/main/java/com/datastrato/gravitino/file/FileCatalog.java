/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.FileAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFileException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import java.util.Map;

/**
 * The FileCatalog interface defines the public API for managing file objects in a schema. If the
 * catalog implementation supports file objects, it should implement this interface.
 */
public interface FileCatalog {

  /**
   * List the files in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of file identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listFiles(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load file metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A file identifier.
   * @return The file metadata.
   * @throws NoSuchFileException If the file does not exist.
   */
  File loadFile(NameIdentifier ident) throws NoSuchFileException;

  /**
   * Check if a file exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A file identifier.
   * @return true If the file exists, false otherwise.
   */
  default boolean fileExists(NameIdentifier ident) {
    try {
      loadFile(ident);
      return true;
    } catch (NoSuchFileException e) {
      return false;
    }
  }

  /**
   * Create a file metadata in the catalog.
   *
   * <p>If the type of the file object is "MANAGED", the underlying storageLocation can be null, and
   * Gravitino will manage the storage location based on the location of the schema.
   *
   * <p>If the type of the file object is "EXTERNAL", the underlying storageLocation must be set.
   *
   * @param ident A file identifier.
   * @param comment The comment of the file.
   * @param type The type of the file.
   * @param storageLocation The storage location of the file.
   * @param properties The properties of the file.
   * @return The created file metadata
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FileAlreadyExistsException If the file already exists.
   */
  File createFile(
      NameIdentifier ident,
      String comment,
      File.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FileAlreadyExistsException;

  /**
   * Apply the {@link FileChange change} to a file in the catalog.
   *
   * <p>Implementation may reject the change. If any change is rejected, no changes should be
   * applied to the file.
   *
   * @param ident A file identifier.
   * @param changes The changes to apply to the file.
   * @return The altered file metadata.
   * @throws NoSuchFileException If the file does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  File alterFile(NameIdentifier ident, FileChange... changes)
      throws NoSuchFileException, IllegalArgumentException;

  /**
   * Drop a file from the catalog.
   *
   * <p>The underlying files will be deleted if this file is managed, otherwise, only the metadata
   * will be dropped.
   *
   * @param ident A file identifier.
   * @return true If the file is dropped, false the file did not exist.
   */
  boolean dropFile(NameIdentifier ident);
}
