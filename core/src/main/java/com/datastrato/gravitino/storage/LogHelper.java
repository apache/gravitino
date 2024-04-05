/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.google.common.annotations.VisibleForTesting;

/**
 * LogHelper is a utility class that provides logging-related functionality for both KV and Relational or other backends in the storage.
 * It holds information about an entity such as its identifier, type, and creation time.
 * The class also provides a NONE instance which is an implementation of the Null Object pattern.
 * This instance can be used to avoid null checks and NullPointerExceptions.
 */
public class LogHelper {

  @VisibleForTesting
  public final NameIdentifier identifier;
  @VisibleForTesting
  public final Entity.EntityType type;
  @VisibleForTesting
  public final long createTimeInMs;
  // Formatted createTime
  @VisibleForTesting
  public final String createTimeAsString;

  public static final LogHelper NONE = new LogHelper(null, null, 0L, null);

  public LogHelper(
      NameIdentifier identifier,
      Entity.EntityType type,
      long createTimeInMs,
      String createTimeAsString) {
    this.identifier = identifier;
    this.type = type;
    this.createTimeInMs = createTimeInMs;
    this.createTimeAsString = createTimeAsString;
  }
}