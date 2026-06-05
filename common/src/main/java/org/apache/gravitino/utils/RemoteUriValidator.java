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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;

/** Validates remote URI hosts before server-side downloads. */
public final class RemoteUriValidator {

  private RemoteUriValidator() {}

  /**
   * Resolves the host in the given URI and rejects local or private addresses unless explicitly
   * allowed.
   *
   * @param uri The remote URI to validate.
   * @param allowLocalAddress Whether local and private addresses are allowed.
   * @param allowLocalAddressHint The configuration hint that enables local and private addresses.
   * @throws IOException If host resolution fails.
   * @throws IllegalArgumentException If the URI has no host or resolves to a blocked address.
   */
  public static void validate(URI uri, boolean allowLocalAddress, String allowLocalAddressHint)
      throws IOException {
    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("URI has no host: " + uri);
    }

    if (allowLocalAddress) {
      return;
    }

    InetAddress[] addresses = InetAddress.getAllByName(host);
    for (InetAddress address : addresses) {
      if (isBlockedAddress(address)) {
        throw new IllegalArgumentException(
            String.format(
                "URI '%s' resolves to blocked address %s from the Gravitino server side. "
                    + "Access to local, private, link-local, multicast, unspecified, and cloud "
                    + "metadata addresses is disabled by default to prevent SSRF. If this URI is "
                    + "trusted and this access is required, set %s.",
                uri, address.getHostAddress(), allowLocalAddressHint));
      }
    }
  }

  private static boolean isBlockedAddress(InetAddress address) {
    if (address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()
        || address.isAnyLocalAddress()) {
      return true;
    }

    byte[] bytes = address.getAddress();
    if (isCloudMetadataAddress(bytes)) {
      return true;
    }

    return isIpv6UniqueLocalAddress(bytes);
  }

  private static boolean isCloudMetadataAddress(byte[] bytes) {
    return bytes.length == 4
        && (bytes[0] & 0xFF) == 100
        && (bytes[1] & 0xFF) == 100
        && (bytes[2] & 0xFF) == 100
        && (bytes[3] & 0xFF) == 200;
  }

  private static boolean isIpv6UniqueLocalAddress(byte[] bytes) {
    return bytes.length == 16 && ((bytes[0] & 0xFE) == 0xFC);
  }
}
