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

  /**
   * Create a namespace for metalake.
   *
   * @return A namespace for metalake
   */
  public static Namespace ofMetalake() {
    return empty();
  }

  /**
   * Create a namespace for catalog.
   *
   * @param metalake The metalake name
   * @return A namespace for catalog
   */
  public static Namespace ofCatalog(String metalake) {
    return of(metalake);
  }

  /**
   * Create a namespace for schema.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return A namespace for schema
   */
  public static Namespace ofSchema(String metalake, String catalog) {
    return of(metalake, catalog);
  }

  /**
   * Create a namespace for table.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return A namespace for table
   */
  public static Namespace ofTable(String metalake, String catalog, String schema) {
    return of(metalake, catalog, schema);
  }

  /**
   * Create a namespace for fileset.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return A namespace for fileset
   */
  public static Namespace ofFileset(String metalake, String catalog, String schema) {
    return of(metalake, catalog, schema);
  }

  /**
   * Check if the given metalake namespace is legal, throw an {@link IllegalNamespaceException} if
   * it's illegal.
   *
   * @param namespace The metalake namespace
   */
  public static void checkMetalake(Namespace namespace) {
    check(
        namespace != null && namespace.isEmpty(),
        "Metalake namespace must be non-null and empty, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given catalog namespace is legal, throw an {@link IllegalNamespaceException} if
   * it's illegal.
   *
   * @param namespace The catalog namespace
   */
  public static void checkCatalog(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 1,
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given schema namespace is legal, throw an {@link IllegalNamespaceException} if
   * it's illegal.
   *
   * @param namespace The schema namespace
   */
  public static void checkSchema(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 2,
        "Schema namespace must be non-null and have 2 levels, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given table namespace is legal, throw an {@link IllegalNamespaceException} if it's
   * illegal.
   *
   * @param namespace The table namespace
   */
  public static void checkTable(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 3,
        "Table namespace must be non-null and have 3 levels, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given fileset namespace is legal, throw an {@link IllegalNamespaceException} if
   * it's illegal.
   *
   * @param namespace The fileset namespace
   */
  public static void checkFileset(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 3,
        "Fileset namespace must be non-null and have 3 levels, the input namespace is %s",
        namespace);
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
