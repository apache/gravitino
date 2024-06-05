/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.IllegalNamespaceException;
import com.google.common.base.Joiner;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Arrays;

/**
 * A namespace is a sequence of levels separated by dots. It's used to identify a metalake, a
 * catalog or a schema. For example, "metalake1", "metalake1.catalog1" and
 * "metalake1.catalog1.schema1" are all valid namespaces.
 */
public class Namespace {

  private static final Namespace EMPTY = new Namespace(new String[0]);
  private static final Joiner DOT = Joiner.on('.');

  private final String[] levels;

  /**
   * Get an empty namespace.
   *
   * @return An empty namespace
   */
  public static Namespace empty() {
    return EMPTY;
  }

  /**
   * Create a namespace with the given levels.
   *
   * @param levels The levels of the namespace
   * @return A namespace with the given levels
   */
  public static Namespace of(String... levels) {
    check(levels != null, "Cannot create a namespace with null levels");
    if (levels.length == 0) {
      return empty();
    }

    for (String level : levels) {
      check(
          level != null && !level.isEmpty(), "Cannot create a namespace with null or empty level");
    }

    return new Namespace(levels);
  }

  private Namespace(String[] levels) {
    this.levels = levels;
  }

  /**
   * Get the levels of the namespace.
   *
   * @return The levels of the namespace
   */
  public String[] levels() {
    return levels;
  }

  /**
   * Get the level at the given position.
   *
   * @param pos The position of the level
   * @return The level at the given position
   */
  public String level(int pos) {
    check(pos >= 0 && pos < levels.length, "Invalid level position");
    return levels[pos];
  }

  /**
   * Get the length of the namespace.
   *
   * @return The length of the namespace.
   */
  public int length() {
    return levels.length;
  }

  /**
   * Check if the namespace is empty.
   *
   * @return True if the namespace is empty, false otherwise.
   */
  public boolean isEmpty() {
    return levels.length == 0;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Namespace)) {
      return false;
    }

    Namespace otherNamespace = (Namespace) other;
    return Arrays.equals(levels, otherNamespace.levels);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(levels);
  }

  @Override
  public String toString() {
    return DOT.join(levels);
  }

  /**
   * Check the given condition is true. Throw an {@link IllegalNamespaceException} if it's not.
   *
   * @param expression The expression to check.
   * @param message The message to throw.
   * @param args The arguments to the message.
   */
  @FormatMethod
  public static void check(boolean expression, @FormatString String message, Object... args) {
    if (!expression) {
      throw new IllegalNamespaceException(message, args);
    }
  }
}
