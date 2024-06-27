/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.io.Closeable;
import java.io.IOException;

/**
 * The abstract base class for Authorization implementations.
 *
 * <p>A typical authorization always contain {@link AuthorizationHook} which is used to trigger the
 * specific operations by the authorization.
 *
 * <p>For example, a Ranger authorization has a RangerAuthorizationHook which manipulates Ranger to
 * management Hive and HDFS permission.
 *
 * @param <T> The type of the concrete subclass of BaseAuthorization.
 */
public abstract class BaseAuthorization<T extends BaseAuthorization>
    implements AuthorizationProvider, Closeable {
  private volatile AuthorizationHook hook = null;

  /**
   * Creates a new instance of AuthorizationHook. The child class should implement this method to
   * provide a specific AuthorizationHook instance regarding that authorization.
   *
   * @return A new instance of AuthorizationHook.
   */
  protected abstract AuthorizationHook newHook();

  public AuthorizationHook hook() {
    if (hook == null) {
      synchronized (this) {
        if (hook == null) {
          hook = newHook();
        }
      }
    }

    return hook;
  }

  @Override
  public void close() throws IOException {
    if (hook != null) {
      hook = null;
    }
  }
}
