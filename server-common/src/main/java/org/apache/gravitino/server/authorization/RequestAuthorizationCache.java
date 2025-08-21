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

import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Used to avoid duplicate authorization checks for the same metadata within the same thread.
 *
 * <p>The time complexity of Jcasbin's enforce method is O(n). Further caching of authorization
 * results can reduce the time spent on authorization.
 */
public class RequestAuthorizationCache {

  private RequestAuthorizationCache() {}

  /** Used to cache the results of metadata authorization. */
  private static final ThreadLocal<Map<AuthorizationContext, Boolean>> allowAuthorizerThreadCache =
      new ThreadLocal<>();

  /** Used to cache the results of metadata authorization. */
  private static final ThreadLocal<Map<AuthorizationContext, Boolean>> denyAuthorizerThreadCache =
      new ThreadLocal<>();

  /** Used to determine whether the role has already been loaded. */
  private static final ThreadLocal<BooleanHolder> hasLoadedRoleThreadLocal = new ThreadLocal<>();

  /**
   * Wrap the authorization method with this method to prevent threadlocal leakage.
   *
   * @param supplier authorization method
   * @return authorization result
   * @param <T> authorization method
   */
  public static <T> T executeWithThreadCache(Supplier<T> supplier) {
    start();
    T result = threadLocalTransmitWrapper(supplier).get();
    end();
    return result;
  }

  /**
   * Used to wrap a Supplier to ensure that the ThreadLocal context can be correctly passed across
   * threads, enabling pruning effects when concurrently obtaining authorization results.
   *
   * @param supplier supplier
   * @return wrapped supplier
   */
  public static <T> Supplier<T> threadLocalTransmitWrapper(Supplier<T> supplier) {
    Map<AuthorizationContext, Boolean> tempAllowAuthorizer = allowAuthorizerThreadCache.get();
    Map<AuthorizationContext, Boolean> tempDenyAuthorizer = denyAuthorizerThreadCache.get();
    BooleanHolder tempRoleLoadedThread = hasLoadedRoleThreadLocal.get();
    Principal currentPrincipal = PrincipalUtils.getCurrentPrincipal();

    return () -> {
      allowAuthorizerThreadCache.set(tempAllowAuthorizer);
      denyAuthorizerThreadCache.set(tempDenyAuthorizer);
      hasLoadedRoleThreadLocal.set(tempRoleLoadedThread);
      T result;
      try {
        result = PrincipalUtils.doAs(currentPrincipal, supplier::get);
        return result;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        allowAuthorizerThreadCache.remove();
        denyAuthorizerThreadCache.remove();
        hasLoadedRoleThreadLocal.remove();
      }
    };
  }

  public static void loadRole(Runnable runnable) {
    BooleanHolder hasLoadedRole = hasLoadedRoleThreadLocal.get();
    if (hasLoadedRole == null) {
      runnable.run();
      return;
    }
    if (hasLoadedRole.isBool()) {
      return;
    }
    runnable.run();
    hasLoadedRole.setBool(true);
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

  private static void start() {
    allowAuthorizerThreadCache.set(new ConcurrentHashMap<>());
    denyAuthorizerThreadCache.set(new ConcurrentHashMap<>());
    hasLoadedRoleThreadLocal.set(new BooleanHolder());
  }

  private static void end() {
    allowAuthorizerThreadCache.remove();
    denyAuthorizerThreadCache.remove();
    hasLoadedRoleThreadLocal.remove();
  }

  /**
   * Boolean cannot be passed to other threads; use a BooleanHolder to hold a reference to the
   * Boolean object, ensuring the Boolean value can be correctly and accurately passed between
   * threads.
   */
  private static class BooleanHolder {
    Boolean bool;

    public Boolean isBool() {
      return bool != null && bool;
    }

    public void setBool(Boolean bool) {
      this.bool = bool;
    }
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
      if (!(o instanceof AuthorizationContext)) {
        return false;
      }
      AuthorizationContext that = (AuthorizationContext) o;
      return Objects.equals(username, that.username)
          && Objects.equals(metalake, that.metalake)
          && Objects.equals(metadataObject, that.metadataObject)
          && Objects.equals(privilege, that.privilege);
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
