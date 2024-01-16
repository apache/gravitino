/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.file;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Namespace;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a fileset under a schema {@link Namespace}. A fileset is a virtual
 * concept of the file or directory that is managed by Gravitino. Users can create a fileset object
 * to manage the non-tabular data on the FS-like storage. The typical use case is to manage the
 * training data for AI workloads. The major difference compare to the relational table is that the
 * fileset is schema-free, the main property of the fileset is the storage location of the
 * underlying data.
 *
 * <p>{@link Fileset} defines the basic properties of a fileset object. A catalog implementation
 * with {@link FilesetCatalog} should implement this interface.
 */
public interface Fileset extends Auditable {

  /** An enum representing the type of the fileset object. */
  enum Type {
    MANAGED, // Fileset is managed by Gravitino. When specified, the data will be deleted when the
    // fileset object is deleted.
    EXTERNAL // Fileset is not managed by Gravitino. When specified, the data will not be deleted
    // when the fileset object is deleted.
  }

  /** @return Name of the fileset object. */
  String name();

  /** @return The comment of the fileset object. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The type of the fileset object. */
  Type type();

  /**
   * Get the storage location of the file or directory path that is managed by this fileset object.
   *
   * @return The storage location of the fileset object.
   */
  String storageLocation();

  /**
   * @return The properties of the fileset object. Empty map is returned if no properties are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
