/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import java.time.Instant;

/** Represents the audit information of an entity. */
public interface Audit {

  /** The creator of the entity. */
  String creator();

  /** The creation time of the entity. */
  Instant createTime();

  /** The last modifier of the entity. */
  String lastModifier();

  /** The last modified time of the entity. */
  Instant lastModifiedTime();
}
