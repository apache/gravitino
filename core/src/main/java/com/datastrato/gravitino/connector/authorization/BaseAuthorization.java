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
package com.datastrato.gravitino.connector.authorization;

import com.datastrato.gravitino.authorization.AuthorizationProvider;
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
      hook.close();
    }
  }
}
