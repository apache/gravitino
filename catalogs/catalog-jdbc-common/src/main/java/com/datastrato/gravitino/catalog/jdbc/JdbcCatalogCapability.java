/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class JdbcCatalogCapability implements Capability {
  /**
   * Regular expression explanation: Regex that matches any string that maybe a filename with an
   * optional extension We adopt a blacklist approach that excludes filename or extension that
   * contains '.', '/', or '\' ^[^.\/\\]+(\.[^.\/\\]+)?$
   *
   * <p>^ - Start of the string
   *
   * <p>[^.\/\\]+ - matches any filename string that does not contain '.', '/', or '\'
   *
   * <p>(\.[^.\/\\]+)? - matches an optional extension
   *
   * <p>$ - End of the string
   */
  // We use sqlite name pattern to be the default pattern for JDBC catalog for testing purposes
  public static final String SQLITE_NAME_PATTERN = "^[^.\\/\\\\]+(\\.[^.\\/\\\\]+)?$";

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    // TODO: Validate the name against reserved words
    if (!name.matches(SQLITE_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }
    return CapabilityResult.SUPPORTED;
  }
}
