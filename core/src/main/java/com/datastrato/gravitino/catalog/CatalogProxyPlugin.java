/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.utils.Executable;
import java.security.Principal;
import java.util.Map;

public interface CatalogProxyPlugin {
  Object doAs(
      Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
      throws Throwable;
}
