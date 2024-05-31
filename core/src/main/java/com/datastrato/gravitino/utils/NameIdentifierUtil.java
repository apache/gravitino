/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;

/**
 * A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
 * catalog, a schema or a table. For example, "metalake1" can represent a metalake,
 * "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
 * schema.
 */
public class NameIdentifierUtil {

  /**
   * Create the metalake {@link NameIdentifierUtil} with the given name.
   *
   * @param metalake The metalake name
   * @return The created metalake {@link NameIdentifier}
   */
  public static NameIdentifier ofMetalake(String metalake) {
    return NameIdentifier.of(metalake);
  }

  /**
   * Create the catalog {@link NameIdentifierUtil} with the given metalake and catalog name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return The created catalog {@link NameIdentifier}
   */
  public static NameIdentifier ofCatalog(String metalake, String catalog) {
    return NameIdentifier.of(metalake, catalog);
  }

  /**
   * Create the schema {@link NameIdentifierUtil} with the given metalake, catalog and schema name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return The created schema {@link NameIdentifierUtil}
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
   * Create the topic {@link NameIdentifier} with the given metalake, catalog, schema and topic
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param topic The topic name
   * @return The created topic {@link NameIdentifier}
   */
  public static NameIdentifier ofTopic(
      String metalake, String catalog, String schema, String topic) {
    return NameIdentifier.of(metalake, catalog, schema, topic);
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
    NameIdentifier.check(ident != null, "Metalake identifier must not be null");
    Namespace.checkMetalake(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a catalog identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The catalog {@link NameIdentifier} to check.
   */
  public static void checkCatalog(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Catalog identifier must not be null");
    Namespace.checkCatalog(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a schema identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The schema {@link NameIdentifier} to check.
   */
  public static void checkSchema(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Schema identifier must not be null");
    Namespace.checkSchema(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a table identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The table {@link NameIdentifier} to check.
   */
  public static void checkTable(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Table identifier must not be null");
    Namespace.checkTable(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a fileset identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The fileset {@link NameIdentifier} to check.
   */
  public static void checkFileset(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Fileset identifier must not be null");
    Namespace.checkFileset(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a topic identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The topic {@link NameIdentifier} to check.
   */
  public static void checkTopic(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Topic identifier must not be null");
    Namespace.checkTopic(ident.namespace());
  }
}
