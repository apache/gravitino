/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.utils;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalNamespaceException;

/** Utility class for namespace. */
public class NamespaceUtil {

  private NamespaceUtil() {}

  /**
   * Create a namespace for metalake.
   *
   * @return A namespace for metalake
   */
  public static Namespace ofMetalake() {
    return Namespace.empty();
  }

  /**
   * Create a namespace for catalog.
   *
   * @param metalake The metalake name
   * @return A namespace for catalog
   */
  public static Namespace ofCatalog(String metalake) {
    return Namespace.of(metalake);
  }

  /**
   * Create a namespace for schema.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return A namespace for schema
   */
  public static Namespace ofSchema(String metalake, String catalog) {
    return Namespace.of(metalake, catalog);
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
    return Namespace.of(metalake, catalog, schema);
  }

  /**
   * Create a namespace for tag.
   *
   * @param metalake The metalake name
   * @return A namespace for tag
   */
  public static Namespace ofTag(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
  }

  /**
   * Create a namespace for policy.
   *
   * @param metalake The metalake name
   * @return A namespace for policy
   */
  public static Namespace ofPolicy(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.POLICY_SCHEMA_NAME);
  }

  /**
   * Create a namespace for user.
   *
   * @param metalake The metalake name
   * @return A namespace for user
   */
  public static Namespace ofUser(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME);
  }

  /**
   * Create a namespace for role.
   *
   * @param metalake The metalake name
   * @return A namespace for role
   */
  public static Namespace ofRole(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME);
  }

  /**
   * Create a namespace for group.
   *
   * @param metalake The metalake name
   * @return A namespace for group
   */
  public static Namespace ofGroup(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME);
  }

  /**
   * Create a namespace for column.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @return A namespace for column
   */
  public static Namespace ofColumn(String metalake, String catalog, String schema, String table) {
    return Namespace.of(metalake, catalog, schema, table);
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
    return Namespace.of(metalake, catalog, schema);
  }

  /**
   * Create a namespace for topic.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return A namespace for topic
   */
  public static Namespace ofTopic(String metalake, String catalog, String schema) {
    return Namespace.of(metalake, catalog, schema);
  }

  /**
   * Create a namespace for model.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return A namespace for model
   */
  public static Namespace ofModel(String metalake, String catalog, String schema) {
    return Namespace.of(metalake, catalog, schema);
  }

  /**
   * Create a namespace for model version.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param model The model name
   * @return A namespace for model version
   */
  public static Namespace ofModelVersion(
      String metalake, String catalog, String schema, String model) {
    return Namespace.of(metalake, catalog, schema, model);
  }

  /**
   * Create a namespace for job template.
   *
   * @param metalake The metalake name
   * @return A namespace for job template
   */
  public static Namespace ofJobTemplate(String metalake) {
    return Namespace.of(
        metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.JOB_TEMPLATE_SCHEMA_NAME);
  }

  /**
   * Create a namespace for job.
   *
   * @param metalake The metalake name
   * @return A namespace for job
   */
  public static Namespace ofJob(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.JOB_SCHEMA_NAME);
  }

  /**
   * Convert a model name identifier to a model version namespace.
   *
   * @param modelIdent The model name identifier
   * @return A model version namespace
   */
  public static Namespace toModelVersionNs(NameIdentifier modelIdent) {
    return ofModelVersion(
        modelIdent.namespace().level(0),
        modelIdent.namespace().level(1),
        modelIdent.namespace().level(2),
        modelIdent.name());
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
   * Check if the given column namespace is legal, throw an {@link IllegalNamespaceException} if
   * it's illegal.
   *
   * @param namespace The column namespace
   */
  public static void checkColumn(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 4,
        "Column namespace must be non-null and have 4 levels, the input namespace is %s",
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

  /**
   * Check if the given topic namespace is legal, throw an {@link IllegalNamespaceException} if it's
   * illegal.
   *
   * @param namespace The topic namespace
   */
  public static void checkTopic(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 3,
        "Topic namespace must be non-null and have 3 levels, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given model namespace is legal, throw an {@link IllegalNamespaceException} if it's
   * illegal.
   *
   * @param namespace The model namespace
   */
  public static void checkModel(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 3,
        "Model namespace must be non-null and have 3 levels, the input namespace is %s",
        namespace);
  }

  /**
   * Check if the given model version namespace is legal, throw an {@link IllegalNamespaceException}
   * if it's illegal.
   *
   * @param namespace The model version namespace
   */
  public static void checkModelVersion(Namespace namespace) {
    check(
        namespace != null && namespace.length() == 4,
        "Model version namespace must be non-null and have 4 levels, the input namespace is %s",
        namespace);
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
