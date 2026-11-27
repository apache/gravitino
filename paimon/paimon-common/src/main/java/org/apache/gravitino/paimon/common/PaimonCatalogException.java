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
package org.apache.gravitino.paimon.common;

import java.util.Locale;
import javax.annotation.Nullable;

/** A catalog operation failure with optional identification of the affected resource. */
public final class PaimonCatalogException extends RuntimeException {
  /** Catalog operations group failures by their meaning to callers. */
  public enum Reason {
    /** The operation references a missing resource. */
    NOT_FOUND,
    /** The resource already exists. */
    ALREADY_EXISTS,
    /** An input or precondition was invalid. */
    INVALID,
    /** The backend does not support the operation. */
    UNSUPPORTED,
    /** The backend could not complete the operation. */
    FAILURE
  }

  /** Types of resources processed by the backend catalog. */
  public enum Resource {
    /** A Paimon database. */
    DATABASE,
    /** A Paimon table. */
    TABLE,
    /** A table column. */
    COLUMN,
    /** A table snapshot. */
    SNAPSHOT,
    /** A table branch. */
    BRANCH,
    /** A table tag. */
    TAG
  }

  private final Reason reason;
  @Nullable private final Resource resource;
  @Nullable private final String resourceName;

  private PaimonCatalogException(
      Reason reason,
      @Nullable Resource resource,
      @Nullable String resourceName,
      String format,
      Object... args) {
    super(String.format(Locale.ROOT, format, args));
    this.reason = reason;
    this.resource = resource;
    this.resourceName = resourceName;
  }

  /**
   * @return the reason for the failure
   */
  public Reason reason() {
    return reason;
  }

  /**
   * @return the affected resource type, if present
   */
  @Nullable
  public Resource resource() {
    return resource;
  }

  /**
   * @return the affected resource name, if present
   */
  @Nullable
  public String resourceName() {
    return resourceName;
  }

  static PaimonCatalogException notFound(
      Resource resource, @Nullable String name, String message, Object... args) {
    return new PaimonCatalogException(Reason.NOT_FOUND, resource, name, message, args);
  }

  static PaimonCatalogException alreadyExists(
      Resource resource, @Nullable String name, String message, Object... args) {
    return new PaimonCatalogException(Reason.ALREADY_EXISTS, resource, name, message, args);
  }

  static PaimonCatalogException invalid(String message, Object... args) {
    return new PaimonCatalogException(Reason.INVALID, null, null, message, args);
  }

  static PaimonCatalogException unsupported(String message, Object... args) {
    return new PaimonCatalogException(Reason.UNSUPPORTED, null, null, message, args);
  }

  static PaimonCatalogException failure(String message, Object... args) {
    return new PaimonCatalogException(Reason.FAILURE, null, null, message, args);
  }
}
