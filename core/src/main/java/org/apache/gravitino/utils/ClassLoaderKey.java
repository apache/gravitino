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
package org.apache.gravitino.utils;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Key used to identify a shared ClassLoader in the {@link ClassLoaderPool}. ClassLoaders are shared
 * among catalogs that have the same provider, package property, authorization plugin path, and
 * Kerberos identity.
 *
 * <p>Kerberos principal and keytab are included because Hadoop's {@code UserGroupInformation} and
 * other static state are scoped to the ClassLoader. Catalogs with different Kerberos identities
 * must use separate ClassLoaders to avoid authentication state corruption.
 */
public class ClassLoaderKey {

  private final String provider;
  @Nullable private final String packageProperty;
  @Nullable private final String authorizationPkgPath;
  @Nullable private final String kerberosPrincipal;
  @Nullable private final String kerberosKeytab;

  /**
   * Constructs a ClassLoaderKey.
   *
   * @param provider The catalog provider name (e.g., "hive", "lakehouse-iceberg").
   * @param packageProperty The catalog package property, or null if not set.
   * @param authorizationPkgPath The authorization plugin package path, or null if not set.
   * @param kerberosPrincipal The Kerberos principal, or null if not using Kerberos.
   * @param kerberosKeytab The Kerberos keytab URI, or null if not using Kerberos.
   */
  public ClassLoaderKey(
      String provider,
      @Nullable String packageProperty,
      @Nullable String authorizationPkgPath,
      @Nullable String kerberosPrincipal,
      @Nullable String kerberosKeytab) {
    this.provider = Objects.requireNonNull(provider, "provider must not be null");
    this.packageProperty = packageProperty;
    this.authorizationPkgPath = authorizationPkgPath;
    this.kerberosPrincipal = kerberosPrincipal;
    this.kerberosKeytab = kerberosKeytab;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ClassLoaderKey)) return false;
    ClassLoaderKey that = (ClassLoaderKey) o;
    return Objects.equals(provider, that.provider)
        && Objects.equals(packageProperty, that.packageProperty)
        && Objects.equals(authorizationPkgPath, that.authorizationPkgPath)
        && Objects.equals(kerberosPrincipal, that.kerberosPrincipal)
        && Objects.equals(kerberosKeytab, that.kerberosKeytab);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        provider, packageProperty, authorizationPkgPath, kerberosPrincipal, kerberosKeytab);
  }

  @Override
  public String toString() {
    return "ClassLoaderKey{"
        + "provider='"
        + provider
        + '\''
        + ", packageProperty='"
        + packageProperty
        + '\''
        + ", authorizationPkgPath='"
        + authorizationPkgPath
        + '\''
        + ", kerberosPrincipal='"
        + kerberosPrincipal
        + '\''
        + ", kerberosKeytab='"
        + kerberosKeytab
        + '\''
        + '}';
  }
}
