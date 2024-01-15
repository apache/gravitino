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

  /** An enum representing the format of the file. */
  enum Format {
    CSV,
    JSON,
    PARQUET,
    AVRO,
    ORC,
    TEXT,
    BINARY,
    IMAGE,
    TFRECORD,
    UNKNOWN
  }

  /** @return Name of the file object. */
  String name();

  /** @return The comment of the file object. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return the format of the underlying filesets that is managed by this file object. */
  Format format();

  /** @return The properties of the file object. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
