/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

/**
 * An Authorization provider is a class that provides a short name for an authorization. This short
 * name is used when creating an authorization.
 */
public interface AuthorizationProvider {
  /**
   * The string that represents the authorization that this provider uses. This is overridden by
   * children to provide a nice alias for the authorization.
   *
   * @return The string that represents the authorization that this provider uses.
   */
  String shortName();
}
