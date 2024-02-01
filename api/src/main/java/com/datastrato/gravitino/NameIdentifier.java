/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Arrays;

/**
 * A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
 * catalog, a schema or a table. For example, "metalake1" can represent a metalake,
 * "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
 * schema.
 */
public class NameIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;

  private final String name;

  /**
   * Create the {@link NameIdentifier} with the given levels of names.
   *
   * @param names The names of the identifier
   * @return The created {@link NameIdentifier}
   */
  public static NameIdentifier of(String... names) {
    check(names != null, "Cannot create a NameIdentifier with null names");
    check(names.length > 0, "Cannot create a NameIdentifier with no names");

    return new NameIdentifier(
        Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  /**
   * Create the {@link NameIdentifier} with the given {@link Namespace} and name.
   *
   * @param namespace The namespace of the identifier
   * @param name The name of the identifier
   * @return The created {@link NameIdentifier}
   */
  public static NameIdentifier of(Namespace namespace, String name) {
    return new NameIdentifier(namespace, name);
  }

  /**
   * Create the metalake {@link NameIdentifier} with the given name.
   *
   * @param metalake The metalake name
   * @return The created metalake {@link NameIdentifier}
   */
  public static NameIdentifier ofMetalake(String metalake) {
    return NameIdentifier.of(metalake);
  }

  /**
   * Create the catalog {@link NameIdentifier} with the given metalake and catalog name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return The created catalog {@link NameIdentifier}
   */
  public static NameIdentifier ofCatalog(String metalake, String catalog) {
    return NameIdentifier.of(metalake, catalog);
  }

  /**
   * Create the schema {@link NameIdentifier} with the given metalake, catalog and schema name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return The created schema {@link NameIdentifier}
   */
  public static NameIdentifier ofSchema(String metalake, String catalog, String schema) {
    return NameIdentifier.of(metalake, catalog, schema);
  }

  /**
   * Create the table {@link NameIdentifier} with the given metalake, catalog, schema and table
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @return The created table {@link NameIdentifier}
   */
  public static NameIdentifier ofTable(
      String metalake, String catalog, String schema, String table) {
    return NameIdentifier.of(metalake, catalog, schema, table);
  }

  /**
   * Create the fileset {@link NameIdentifier} with the given metalake, catalog, schema and fileset
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param fileset The fileset name
   * @return The created fileset {@link NameIdentifier}
   */
  public static NameIdentifier ofFileset(
      String metalake, String catalog, String schema, String fileset) {
    return NameIdentifier.of(metalake, catalog, schema, fileset);
  }

  /**
   * Check the given {@link NameIdentifier} is a metalake identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The metalake {@link NameIdentifier} to check.
   */
  public static void checkMetalake(NameIdentifier ident) {
    // We don't have to check the name field of NameIdentifier, it's already checked when
    // creating NameIdentifier object.
    check(ident != null, "Metalake identifier must not be null");
    Namespace.checkMetalake(ident.namespace);
  }

  /**
   * Check the given {@link NameIdentifier} is a catalog identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The catalog {@link NameIdentifier} to check.
   */
  public static void checkCatalog(NameIdentifier ident) {
    check(ident != null, "Catalog identifier must not be null");
    Namespace.checkCatalog(ident.namespace);
  }

  /**
   * Check the given {@link NameIdentifier} is a schema identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The schema {@link NameIdentifier} to check.
   */
  public static void checkSchema(NameIdentifier ident) {
    check(ident != null, "Schema identifier must not be null");
    Namespace.checkSchema(ident.namespace);
  }

  /**
   * Check the given {@link NameIdentifier} is a table identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The table {@link NameIdentifier} to check.
   */
  public static void checkTable(NameIdentifier ident) {
    check(ident != null, "Table identifier must not be null");
    Namespace.checkTable(ident.namespace);
  }

  /**
   * Check the given {@link NameIdentifier} is a fileset identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The fileset {@link NameIdentifier} to check.
   */
  public static void checkFileset(NameIdentifier ident) {
    check(ident != null, "Fileset identifier must not be null");
    Namespace.checkFileset(ident.namespace);
  }

  /**
   * Create a {@link NameIdentifier} from the given identifier string.
   *
   * @param identifier The identifier string
   * @return The created {@link NameIdentifier}
   */
  public static NameIdentifier parse(String identifier) {
    check(identifier != null && !identifier.isEmpty(), "Cannot parse a null or empty identifier");

    Iterable<String> parts = DOT.split(identifier);
    return NameIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private NameIdentifier(Namespace namespace, String name) {
    check(namespace != null, "Cannot create a NameIdentifier with null namespace");
    check(
        name != null && !name.isEmpty(), "Cannot create a NameIdentifier with null or empty name");

    this.namespace = namespace;
    this.name = name;
  }

  /**
   * Check if the {@link NameIdentifier} has a namespace.
   *
   * @return True if the {@link NameIdentifier} has a namespace, false otherwise.
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * Get the namespace of the {@link NameIdentifier}.
   *
   * @return The namespace of the {@link NameIdentifier}.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Get the name of the {@link NameIdentifier}.
   *
   * @return The name of the {@link NameIdentifier}.
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof NameIdentifier)) {
      return false;
    }

    NameIdentifier otherNameIdentifier = (NameIdentifier) other;
    return namespace.equals(otherNameIdentifier.namespace) && name.equals(otherNameIdentifier.name);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new int[] {namespace.hashCode(), name.hashCode()});
  }

  @Override
  public String toString() {
    if (hasNamespace()) {
      return namespace.toString() + "." + name;
    } else {
      return name;
    }
  }

  /**
   * Check the given condition is true. Throw an {@link IllegalNameIdentifierException} if it's not.
   *
   * @param condition The condition to check.
   * @param message The message to throw.
   * @param args The arguments to the message.
   */
  @FormatMethod
  public static void check(boolean condition, @FormatString String message, Object... args) {
    if (!condition) {
      throw new IllegalNameIdentifierException(message, args);
    }
  }
}
