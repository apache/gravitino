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

package org.apache.gravitino.authorization;

import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.gravitino.MetadataObject;

public class AuthorizationRequestContext {

  /** Used to cache the results of metadata authorization. */
  private final Map<AuthorizationKey, Boolean> allowAuthorizerCache = new ConcurrentHashMap<>();

  /** Used to cache the results of metadata authorization. */
  private final Map<AuthorizationKey, Boolean> denyAuthorizerCache = new ConcurrentHashMap<>();

  /** Used to determine whether the role has already been loaded. */
  private final AtomicBoolean hasLoadRole = new AtomicBoolean();

  /**
   * check allow
   *
   * @param principal principal
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public boolean authorizeAllow(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      Function<AuthorizationKey, Boolean> authorizer) {
    AuthorizationKey context = new AuthorizationKey(principal, metalake, metadataObject, privilege);
    return allowAuthorizerCache.computeIfAbsent(context, authorizer);
  }

  /**
   * check deny
   *
   * @param principal principal
   * @param metalake metalake
   * @param metadataObject metadata object
   * @param privilege privilege
   * @param authorizer authorizer
   * @return authorization result
   */
  public boolean authorizeDeny(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      Function<AuthorizationKey, Boolean> authorizer) {
    AuthorizationKey context = new AuthorizationKey(principal, metalake, metadataObject, privilege);
    return denyAuthorizerCache.computeIfAbsent(context, authorizer);
  }

  public void loadRole(Runnable runnable) {
    if (hasLoadRole.get()) {
      return;
    }
    runnable.run();
    hasLoadRole.set(true);
  }

  public static class AuthorizationKey {
    private Principal principal;
    private String metalake;
    private MetadataObject metadataObject;
    private Privilege.Name privilege;

    public AuthorizationKey(
        Principal principal,
        String metalake,
        MetadataObject metadataObject,
        Privilege.Name privilege) {
      this.principal = principal;
      this.metalake = metalake;
      this.metadataObject = metadataObject;
      this.privilege = privilege;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof AuthorizationKey)) {
        return false;
      }
      AuthorizationKey that = (AuthorizationKey) o;
      return Objects.equals(principal, that.principal)
          && Objects.equals(metalake, that.metalake)
          && Objects.equals(metadataObject, that.metadataObject)
          && Objects.equals(privilege, that.privilege);
    }

    @Override
    public int hashCode() {
      return Objects.hash(principal, metalake, metadataObject, privilege);
    }

    public Principal getPrincipal() {
      return principal;
    }

    public void setPrincipal(Principal principal) {
      this.principal = principal;
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

    public Privilege.Name getPrivilege() {
      return privilege;
    }

    public void setPrivilege(Privilege.Name privilege) {
      this.privilege = privilege;
    }
  }
}
