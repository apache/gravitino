/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import com.google.common.base.Preconditions;

/**
 * Holds the server-wide settings the Lance REST authorization code needs.
 *
 * <p>The context is created once while the Lance REST service initializes and read afterwards by
 * the authorization interceptor and the metadata list filter.
 */
public class LanceRESTServerContext {

  private static class InstanceHolder {
    private static LanceRESTServerContext instance;
  }

  private final boolean authorizationEnabled;
  private final boolean auxMode;
  private final String metalakeName;

  private LanceRESTServerContext(
      boolean authorizationEnabled, boolean auxMode, String metalakeName) {
    this.authorizationEnabled = authorizationEnabled;
    this.auxMode = auxMode;
    this.metalakeName = metalakeName;
  }

  /**
   * Creates the context and publishes it as the current instance.
   *
   * @param enableAuthorization whether authorization is enabled on the Gravitino server.
   * @param auxMode whether Lance REST runs as an auxiliary service inside the Gravitino server.
   * @param metalakeName the metalake Lance REST is bound to.
   * @return the created context.
   */
  public static LanceRESTServerContext create(
      boolean enableAuthorization, boolean auxMode, String metalakeName) {
    InstanceHolder.instance =
        new LanceRESTServerContext(enableAuthorization, auxMode, metalakeName);
    return InstanceHolder.instance;
  }

  /**
   * Returns the current context.
   *
   * @return the current context.
   * @throws IllegalStateException if the context has not been created yet.
   */
  public static LanceRESTServerContext getInstance() {
    Preconditions.checkState(
        InstanceHolder.instance != null, "Lance REST server context is not initialized");
    return InstanceHolder.instance;
  }

  /**
   * Whether metadata authorization is applied to Lance REST requests.
   *
   * <p>Authorization relies on the Gravitino authorizer running in the same process, so it is only
   * applied in auxiliary mode. Standalone deployments keep their previous behavior.
   *
   * @return {@code true} when Lance REST authorizes metadata operations.
   */
  public boolean isAuthorizationEnabled() {
    return authorizationEnabled && auxMode;
  }

  /**
   * Whether Lance REST runs as an auxiliary service inside the Gravitino server.
   *
   * @return {@code true} in auxiliary mode.
   */
  public boolean isAuxMode() {
    return auxMode;
  }

  /**
   * Returns the metalake Lance REST is bound to.
   *
   * @return the metalake name.
   */
  public String metalakeName() {
    return metalakeName;
  }
}
