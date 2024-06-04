/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * The abstract base class for Authorization implementations.
 *
 * <p>A typical authorization always contain {@link AuthorizationOperations} which is used to
 * trigger the specific operations by the authorization.
 *
 * <p>For example, a Ranger authorization has a RangerAuthorizationOperations which manipulates
 * Ranger to management Hive and HDFS permission.
 *
 * @param <T> The type of the concrete subclass of BaseAuthorization.
 */
public abstract class BaseAuthorization<T extends BaseAuthorization>
    implements AuthorizationProvider, Closeable {
  private volatile AuthorizationOperations ops = null;

  private Map<String, String> conf;

  public T withAuthorizationConf(Map<String, String> conf) {
    this.conf = conf;
    return (T) this;
  }

  /**
   * Creates a new instance of AuthorizationOperations. The child class should implement this method
   * to provide a specific AuthorizationOperations instance regarding that authorization.
   *
   * @param config The configuration parameters for creating AuthorizationOperations.
   * @return A new instance of AuthorizationOperations.
   */
  protected abstract AuthorizationOperations newOps(Map<String, String> config);

  public AuthorizationOperations ops() {
    if (ops == null) {
      synchronized (this) {
        if (ops == null) {
          ops = newOps(conf);
        }
      }
    }

    return ops;
  }

  @Override
  public void close() throws IOException {
    if (ops != null) {
      ops = null;
    }
  }
}
