/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
// Referred from Trino's AutoCloseableCloser implementation
// core/trino-main/src/main/java/io/trino/util/AutoCloseableCloser.java
package com.datastrato.gravitino.integration.test.util;

import static com.google.common.base.Throwables.propagateIfPossible;
import static java.util.Objects.requireNonNull;

import java.util.ArrayDeque;
import java.util.Deque;

/** This class is inspired by com.google.common.io.Closer */
public final class CloseableGroup implements AutoCloseable {
  private final Deque<AutoCloseable> stack = new ArrayDeque<>(4);

  private CloseableGroup() {}

  public static CloseableGroup create() {
    return new CloseableGroup();
  }

  public <C extends AutoCloseable> C register(C closeable) {
    requireNonNull(closeable, "closeable is null");
    stack.addFirst(closeable);
    return closeable;
  }

  @Override
  public void close() throws Exception {
    Throwable rootCause = null;
    while (!stack.isEmpty()) {
      AutoCloseable closeable = stack.removeFirst();
      try {
        closeable.close();
      } catch (Throwable t) {
        if (rootCause == null) {
          rootCause = t;
        } else if (rootCause != t) {
          // Self-suppression not permitted
          rootCause.addSuppressed(t);
        }
      }
    }
    if (rootCause != null) {
      propagateIfPossible(rootCause, Exception.class);
      // not possible
      throw new AssertionError(rootCause);
    }
  }
}
