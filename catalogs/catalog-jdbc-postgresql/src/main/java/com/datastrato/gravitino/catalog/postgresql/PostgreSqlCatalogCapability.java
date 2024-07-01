/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class PostgreSqlCatalogCapability implements Capability {
  /**
   * Regular expression explanation: ^[_a-zA-Z\p{L}/][\w\p{L}-$/=]{0,62}$
   *
   * <p>^[_a-zA-Z\p{L}/] - Start with an underscore, a letter, or a letter from any language
   *
   * <p>[\w\p{L}-$/=]{0,62} - Consist of 0 to 62 characters (making the total length at most 63) of
   * letters (both cases), digits, underscores, any kind of letter from any language, hyphens,
   * dollar signs, slashes or equal signs
   *
   * <p>$ - End of the string
   */
  public static final String POSTGRESQL_NAME_PATTERN = "^[_a-zA-Z\\p{L}/][\\w\\p{L}-$/=]{0,62}$";

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    // TODO: Validate the name against reserved words
    if (!name.matches(POSTGRESQL_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }
    return CapabilityResult.SUPPORTED;
  }
}
