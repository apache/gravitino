/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.utils.Executable;
import java.security.Principal;
import java.util.Map;

/** The catalog can implement their own ProxyPlugin to execute operations by given user. */
public interface ProxyPlugin {

  /**
   * @param principal The given principal to execute the action
   * @param action A method need to be executed.
   * @param properties The properties which be used when execute the action.
   * @return The return value of action.
   * @throws Throwable The throwable object which the action throws.
   */
  Object doAs(
      Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
      throws Throwable;
}
