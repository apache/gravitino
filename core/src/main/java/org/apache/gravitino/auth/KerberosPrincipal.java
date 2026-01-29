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

package org.apache.gravitino.auth;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Kerberos principal with its structured components.
 *
 * <p>A Kerberos principal has the format: {@code primary[/instance][@REALM]} where:
 *
 * <ul>
 *   <li><b>primary</b> (required): The primary component, typically the user or service name
 *   <li><b>instance</b> (optional): The secondary component, often a hostname for service
 *       principals
 *   <li><b>realm</b> (optional): The Kerberos realm (domain), typically uppercase
 * </ul>
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code john} - primary only
 *   <li>{@code john@EXAMPLE.COM} - primary with realm
 *   <li>{@code HTTP/server.example.com@EXAMPLE.COM} - service principal with instance and realm
 * </ul>
 */
public class KerberosPrincipal implements Principal {

  private final String username;
  private final String instance;
  private final String realm;
  private final String fullPrincipal;

  /**
   * Creates a new Kerberos principal.
   *
   * @param username the primary username component (required, cannot be null or empty)
   * @param instance the instance component (optional, can be null)
   * @param realm the realm component (optional, can be null)
   * @throws IllegalArgumentException if username is null or empty
   */
  public KerberosPrincipal(String username, String instance, String realm) {
    if (username == null || username.isEmpty()) {
      throw new IllegalArgumentException("Username cannot be null or empty");
    }
    this.username = username;
    this.instance = instance;
    this.realm = realm;
    this.fullPrincipal = buildFullPrincipal(username, instance, realm);
  }

  /**
   * Gets the primary component (username). This is the name used for authentication.
   *
   * @return the username (never null or empty)
   */
  @Override
  public String getName() {
    return username;
  }

  /**
   * Gets the instance (secondary component) of this principal, if present.
   *
   * @return an Optional containing the instance, or empty if no instance
   */
  public Optional<String> getInstance() {
    return Optional.ofNullable(instance);
  }

  /**
   * Gets the realm of this principal, if present.
   *
   * @return an Optional containing the realm, or empty if no realm
   */
  public Optional<String> getRealm() {
    return Optional.ofNullable(realm);
  }

  /**
   * Gets the full principal string in Kerberos format.
   *
   * @return the full principal string (e.g., "user/instance@REALM")
   */
  public String getFullPrincipal() {
    return fullPrincipal;
  }

  /**
   * Gets the primary component with instance (if present), without realm.
   *
   * @return primary/instance (e.g., "HTTP/server") or just primary if no instance
   */
  public String getPrimaryWithInstance() {
    if (instance != null && !instance.isEmpty()) {
      return username + "/" + instance;
    }
    return username;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KerberosPrincipal)) {
      return false;
    }
    KerberosPrincipal that = (KerberosPrincipal) o;
    return Objects.equals(username, that.username)
        && Objects.equals(instance, that.instance)
        && Objects.equals(realm, that.realm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, instance, realm);
  }

  @Override
  public String toString() {
    return "KerberosPrincipal{" + "fullPrincipal='" + fullPrincipal + '\'' + '}';
  }

  /**
   * Builds the full principal string from components.
   *
   * @param username the username component
   * @param instance the instance component (optional)
   * @param realm the realm component (optional)
   * @return the full principal string
   */
  private static String buildFullPrincipal(String username, String instance, String realm) {
    StringBuilder sb = new StringBuilder(username);
    if (instance != null && !instance.isEmpty()) {
      sb.append('/').append(instance);
    }
    if (realm != null && !realm.isEmpty()) {
      sb.append('@').append(realm);
    }
    return sb.toString();
  }
}
