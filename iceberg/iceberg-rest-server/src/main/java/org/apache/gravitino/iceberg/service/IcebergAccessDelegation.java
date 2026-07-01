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
package org.apache.gravitino.iceberg.service;

import org.apache.commons.lang3.StringUtils;

/** Parsed value of the {@code X-Iceberg-Access-Delegation} request header. */
public final class IcebergAccessDelegation {

  private static final IcebergAccessDelegation NONE = new IcebergAccessDelegation(false, false);

  private final boolean requestVendedCredentials;
  private final boolean requestRemoteSigning;

  private IcebergAccessDelegation(boolean requestVendedCredentials, boolean requestRemoteSigning) {
    this.requestVendedCredentials = requestVendedCredentials;
    this.requestRemoteSigning = requestRemoteSigning;
  }

  /**
   * Returns an empty delegation mode when the header is absent.
   *
   * @return delegation mode with no requested access delegation
   */
  public static IcebergAccessDelegation none() {
    return NONE;
  }

  /**
   * Creates a delegation mode from explicit flags.
   *
   * @param requestVendedCredentials whether vended credentials are requested
   * @param requestRemoteSigning whether remote signing is requested
   * @return parsed delegation mode
   */
  public static IcebergAccessDelegation of(
      boolean requestVendedCredentials, boolean requestRemoteSigning) {
    if (!requestVendedCredentials && !requestRemoteSigning) {
      return NONE;
    }
    return new IcebergAccessDelegation(requestVendedCredentials, requestRemoteSigning);
  }

  /**
   * Parses the {@code X-Iceberg-Access-Delegation} header value.
   *
   * @param accessDelegation raw header value, may be comma-separated
   * @return parsed delegation mode
   * @throws IllegalArgumentException if the header contains an unsupported value
   */
  public static IcebergAccessDelegation parse(String accessDelegation) {
    if (StringUtils.isBlank(accessDelegation)) {
      return NONE;
    }

    boolean vended = false;
    boolean remoteSigning = false;
    for (String part : accessDelegation.split(",")) {
      String value = part.trim();
      if (value.isEmpty()) {
        continue;
      }
      if ("vended-credentials".equalsIgnoreCase(value)) {
        vended = true;
      } else if ("remote-signing".equalsIgnoreCase(value)) {
        remoteSigning = true;
      } else {
        throw new IllegalArgumentException(
            "X-Iceberg-Access-Delegation: "
                + accessDelegation
                + " is illegal, Iceberg REST spec supports: [vended-credentials,remote-signing]");
      }
    }

    if (!vended && !remoteSigning) {
      return NONE;
    }
    return new IcebergAccessDelegation(vended, remoteSigning);
  }

  /**
   * Returns whether the client requested vended credentials.
   *
   * @return true when vended credentials are requested
   */
  public boolean requestVendedCredentials() {
    return requestVendedCredentials;
  }

  /**
   * Returns whether the client requested remote signing.
   *
   * @return true when remote signing is requested
   */
  public boolean requestRemoteSigning() {
    return requestRemoteSigning;
  }
}
