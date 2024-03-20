/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import java.time.Instant;

/** Represents the audit information of an entity. */
@Evolving
public interface Audit {

  /**
   * The creator of the entity.
   *
   * @return the creator of the entity.
   */
  String creator();

  /**
   * The creation time of the entity.
   *
   * @return The creation time of the entity.
   */
  Instant createTime();

  /** @return The last modifier of the entity. */
  String lastModifier();

  /** @return The last modified time of the entity. */
  Instant lastModifiedTime();
}
