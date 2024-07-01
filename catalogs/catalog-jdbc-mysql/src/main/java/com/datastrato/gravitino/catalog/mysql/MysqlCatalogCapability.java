/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class MysqlCatalogCapability implements Capability {
  /**
   * Regular expression explanation: ^[\w\p{L}-$/=]{1,64}$
   *
   * <p>^ - Start of the string
   *
   * <p>[\w\p{L}-$/=]{1,64} - Consist of 1 to 64 characters of letters (both cases), digits,
   * underscores, any kind of letter from any language, hyphens, dollar signs, slashes or equal
   * signs
   *
   * <p>\w - matches [a-zA-Z0-9_]
   *
   * <p>\p{L} - matches any kind of letter from any language
   *
   * <p>$ - End of the string
   */
  public static final String MYSQL_NAME_PATTERN = "^[\\w\\p{L}-$/=]{1,64}$";

  @Override
  public CapabilityResult specificationOnName(Scope scope, String name) {
    // TODO: Validate the name against reserved words
    if (!name.matches(MYSQL_NAME_PATTERN)) {
      return CapabilityResult.unsupported(
          String.format("The %s name '%s' is illegal.", scope, name));
    }
    return CapabilityResult.SUPPORTED;
  }
}
