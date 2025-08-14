/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.gravitino.MetadataObject;

/** Used to avoid duplicate authorization checks for the same metadata within the same thread. */
public class ThreadLocalAuthorizationCache {

  private ThreadLocalAuthorizationCache() {}

  /** Used to cache the results of metadata authorization. */
  private static final ThreadLocal<Map<AuthorizationContext, Boolean>> allowAuthorizerThreadCache =
      new ThreadLocal<>();

  /** Used to cache the results of metadata authorization. */
  private static final ThreadLocal<Map<AuthorizationContext, Boolean>> denyAuthorizerThreadCache =
      new ThreadLocal<>();

  /** Used to determine whether the privilege has already been loaded. */
  private static final ThreadLocal<Boolean> hasLoadedPrivilegeThreadLocal = new ThreadLocal<>();

  private static void start() {
    allowAuthorizerThreadCache.set(new HashMap<>());
    denyAuthorizerThreadCache.set(new HashMap<>());
    hasLoadedPrivilegeThreadLocal.set(false);
  }

  /**
   * Wrap the authorization method with this method to prevent threadlocal leakage.
   *
   * @param supplier authorization method
   * @return authorization result
   * @param <T> authorization method
   */
  public static <T> T executeWithThreadCache(Supplier<T> supplier) {
    start();
    T result = supplier.get();
    end();
    return result;
  }

  /**
   * Wrap with this method to avoid repeatedly loading permissions within a single request.
   *
   * @param runnable load privilege method
   */
  public static void loadPrivilege(Runnable runnable) {
    Boolean hasLoadedPrivilege = hasLoadedPrivilegeThreadLocal.get();
    if (hasLoadedPrivilege != null && hasLoadedPrivilege) {
      return;
    }
    runnable.run();
    hasLoadedPrivilegeThreadLocal.set(true);
  }

  /**
   * check allow
   *
   * @param username username
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public static boolean authorizeAllow(
      String username,
      String metalake,
      MetadataObject metadataObject,
      String privilege,
      Function<AuthorizationContext, Boolean> authorizer) {
    AuthorizationContext context =
        new AuthorizationContext(username, metalake, metadataObject, privilege);
    Map<AuthorizationContext, Boolean> authorizationContextBooleanMap =
        allowAuthorizerThreadCache.get();
    if (authorizationContextBooleanMap == null) {
      return authorizer.apply(context);
    }
    return authorizationContextBooleanMap.computeIfAbsent(context, authorizer);
  }

  /**
   * check deny
   *
   * @param username username
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public static boolean authorizeDeny(
      String username,
      String metalake,
      MetadataObject metadataObject,
      String privilege,
      Function<AuthorizationContext, Boolean> authorizer) {
    AuthorizationContext context =
        new AuthorizationContext(username, metalake, metadataObject, privilege);
    Map<AuthorizationContext, Boolean> authorizationContextBooleanMap =
        denyAuthorizerThreadCache.get();
    if (authorizationContextBooleanMap == null) {
      return authorizer.apply(context);
    }
    return authorizationContextBooleanMap.computeIfAbsent(context, authorizer);
  }

  private static void end() {
    allowAuthorizerThreadCache.remove();
    denyAuthorizerThreadCache.remove();
    hasLoadedPrivilegeThreadLocal.remove();
  }

  public static class AuthorizationContext {
    private String username;
    private String metalake;
    private MetadataObject metadataObject;
    private String privilege;

    public AuthorizationContext(
        String username, String metalake, MetadataObject metadataObject, String privilege) {
      this.username = username;
      this.metalake = metalake;
      this.metadataObject = metadataObject;
      this.privilege = privilege;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AuthorizationContext that = (AuthorizationContext) o;
      return Objects.equals(username, that.username)
          && Objects.equals(metalake, that.metalake)
          && Objects.equals(metadataObject, that.metadataObject)
          && privilege == that.privilege;
    }

    @Override
    public int hashCode() {
      return Objects.hash(username, metalake, metadataObject, privilege);
    }

    public String getUsername() {
      return username;
    }

    public void setUsername(String username) {
      this.username = username;
    }

    public String getMetalake() {
      return metalake;
    }

    public void setMetalake(String metalake) {
      this.metalake = metalake;
    }

    public MetadataObject getMetadataObject() {
      return metadataObject;
    }

    public void setMetadataObject(MetadataObject metadataObject) {
      this.metadataObject = metadataObject;
    }

    public String getPrivilege() {
      return privilege;
    }

    public void setPrivilege(String privilege) {
      this.privilege = privilege;
    }
  }
}
