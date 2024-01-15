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
 * An interface representing a file in a schema {@link Namespace}. It defines the basic properties
 * of a file object. A catalog implementation with {@link FileCatalog} should implement this
 * interface.
 */
public interface File extends Auditable {

  /** An enum representing the type of the file object. */
  enum Type {
    MANAGED, // File is managed by Gravitino. When specified, the data will be deleted when the
    // file object is deleted.
    EXTERNAL // File is not managed by Gravitino. When specified, the data will not be deleted when
    // the file object is deleted.
  }

  /** @return Name of the file object. */
  String name();

  /** @return The comment of the file object. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The type of the file object. */
  Type type();

  /**
   * Get the storage location of the file or directory path that is managed by this file object.
   *
   * @return The storage location of the file object.
   */
  String storageLocation();

  /** @return The properties of the file object. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
