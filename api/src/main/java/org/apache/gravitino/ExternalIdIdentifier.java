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
package org.apache.gravitino;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.Arrays;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;

/**
 * An identifier for entities located by external id within a namespace.
 *
 * <p>This is distinct from {@link NameIdentifier}, which identifies entities by Gravitino name.
 */
public class ExternalIdIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;

  private final String externalId;

  /**
   * Create the {@link ExternalIdIdentifier} with the given levels of names.
   *
   * @param names The names of the identifier
   * @return The created {@link ExternalIdIdentifier}
   */
  public static ExternalIdIdentifier of(String... names) {
    check(names != null, "Cannot create an ExternalIdIdentifier with null names");
    check(names.length > 0, "Cannot create an ExternalIdIdentifier with no names");

    return new ExternalIdIdentifier(
        Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  /**
   * Create the {@link ExternalIdIdentifier} with the given {@link Namespace} and external id.
   *
   * @param namespace The namespace of the identifier
   * @param externalId The external id of the identifier
   * @return The created {@link ExternalIdIdentifier}
   */
  public static ExternalIdIdentifier of(Namespace namespace, String externalId) {
    return new ExternalIdIdentifier(namespace, externalId);
  }

  /**
   * Create a {@link ExternalIdIdentifier} from the given identifier string.
   *
   * @param identifier The identifier string
   * @return The created {@link ExternalIdIdentifier}
   */
  public static ExternalIdIdentifier parse(String identifier) {
    check(identifier != null && !identifier.isEmpty(), "Cannot parse a null or empty identifier");

    Iterable<String> parts = DOT.split(identifier);
    return ExternalIdIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private ExternalIdIdentifier(Namespace namespace, String externalId) {
    check(namespace != null, "Cannot create an ExternalIdIdentifier with null namespace");
    check(
        externalId != null && !externalId.isEmpty(),
        "Cannot create an ExternalIdIdentifier with null or empty external id");

    this.namespace = namespace;
    this.externalId = externalId;
  }

  /**
   * Check if the {@link ExternalIdIdentifier} has a namespace.
   *
   * @return True if the {@link ExternalIdIdentifier} has a namespace, false otherwise.
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * Get the namespace of the {@link ExternalIdIdentifier}.
   *
   * @return The namespace of the {@link ExternalIdIdentifier}.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Get the external id of the {@link ExternalIdIdentifier}.
   *
   * @return The external id of the {@link ExternalIdIdentifier}.
   */
  public String externalId() {
    return externalId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExternalIdIdentifier)) {
      return false;
    }

    ExternalIdIdentifier otherExternalIdIdentifier = (ExternalIdIdentifier) other;
    return namespace.equals(otherExternalIdIdentifier.namespace)
        && externalId.equals(otherExternalIdIdentifier.externalId);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new int[] {namespace.hashCode(), externalId.hashCode()});
  }

  @Override
  public String toString() {
    if (hasNamespace()) {
      return namespace.toString() + "." + externalId;
    } else {
      return externalId;
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
