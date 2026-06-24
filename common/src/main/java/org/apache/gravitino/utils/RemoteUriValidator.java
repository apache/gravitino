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
import java.net.UnknownHostException;

/** Validates remote URI hosts before server-side downloads. */
public final class RemoteUriValidator {

  private RemoteUriValidator() {}

  /**
   * Resolves the host in the given URI exactly once, rejects unsafe addresses, and returns a
   * validated address for the caller to pin the subsequent connection to.
   *
   * <p>Returning the resolved address closes the DNS-rebinding TOCTOU window: a caller that
   * connects to the returned {@link InetAddress} never re-resolves the hostname, so an attacker
   * cannot make validation see a safe address and the fetch see an unsafe one.
   *
   * @param uri The remote URI to validate.
   * @param blockUnsafeAddressHint The configuration hint that disables unsafe address blocking.
   * @return The first resolved address, guaranteed to be safe.
   * @throws IOException If host resolution fails.
   * @throws IllegalArgumentException If the URI has no host or resolves to an unsafe address.
   */
  public static InetAddress resolveAndValidate(URI uri, String blockUnsafeAddressHint)
      throws IOException {
    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("URI has no host: " + SafeUri.redact(uri));
    }

    InetAddress[] addresses = InetAddress.getAllByName(host);
    for (InetAddress address : addresses) {
      if (isUnsafeAddress(address)) {
        throw new IllegalArgumentException(
            String.format(
                "URI '%s' resolves to blocked address %s from the Gravitino server side. "
                    + "Access to local, private, link-local, multicast, unspecified, and cloud "
                    + "metadata addresses is disabled by default to prevent SSRF. If this URI is "
                    + "trusted and this access is required, set %s.",
                SafeUri.redact(uri), address.getHostAddress(), blockUnsafeAddressHint));
      }
    }
    return addresses[0];
  }

  private static boolean isUnsafeAddress(InetAddress address) {
    if (address.isLoopbackAddress()
        || address.isLinkLocalAddress()
        || address.isSiteLocalAddress()
        || address.isMulticastAddress()
        || address.isAnyLocalAddress()) {
      return true;
    }

    byte[] bytes = address.getAddress();
    if (isUnsafeIpv4Address(bytes)) {
      return true;
    }
    if (bytes.length == 16) {
      if (isIpv6UniqueLocalAddress(bytes)) {
        return true;
      }
      // Several IPv6 forms embed an IPv4 address the platform checks above miss (IPv4-compatible,
      // NAT64, 6to4, ISATAP; plus IPv4-mapped defensively). Re-classify the embedded IPv4 so a
      // blocked address such as cloud metadata cannot be reached via an IPv6 literal.
      byte[] embeddedIpv4 = embeddedIpv4(bytes);
      if (embeddedIpv4 != null) {
        try {
          return isUnsafeAddress(InetAddress.getByAddress(embeddedIpv4));
        } catch (UnknownHostException e) {
          // getByAddress only rejects a wrong-length array; a 4-byte array never reaches here. Fail
          // closed if it somehow does.
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns the IPv4 address embedded in a 16-byte IPv6 address whose embedded IPv4 is the literal
   * connection target, or {@code null} if there is none. Covers the IPv4-compatible ({@code
   * ::a.b.c.d}), NAT64 ({@code 64:ff9b::a.b.c.d}, RFC 6052), 6to4 ({@code 2002:a.b.c.d::}, RFC
   * 3056) and ISATAP ({@code ::0:5efe:a.b.c.d}, RFC 5214) forms. The IPv4-mapped ({@code
   * ::ffff:a.b.c.d}) form is handled defensively: the JDK normally surfaces it as a 4-byte {@link
   * java.net.Inet4Address} validated by {@link #isUnsafeIpv4Address}, so this branch is a fallback.
   *
   * <p>Teredo ({@code 2001:0::/32}) is intentionally excluded: its embedded IPv4 is the
   * client/relay identifier rather than the connection destination, and it requires a non-default
   * Teredo tunnel to route at all.
   */
  private static byte[] embeddedIpv4(byte[] bytes) {
    boolean highTenZero = true;
    for (int i = 0; i < 10; i++) {
      if (bytes[i] != 0) {
        highTenZero = false;
        break;
      }
    }
    if (highTenZero) {
      boolean compatible = bytes[10] == 0 && bytes[11] == 0;
      // Defensive: the JDK normally collapses IPv4-mapped addresses to a 4-byte Inet4Address.
      boolean mapped = (bytes[10] & 0xFF) == 0xFF && (bytes[11] & 0xFF) == 0xFF;
      if (compatible || mapped) {
        return lowestFourBytes(bytes);
      }
    }
    // NAT64 well-known prefix 64:ff9b::/96 (RFC 6052): the IPv4 is the low 32 bits.
    if ((bytes[0] & 0xFF) == 0x00
        && (bytes[1] & 0xFF) == 0x64
        && (bytes[2] & 0xFF) == 0xFF
        && (bytes[3] & 0xFF) == 0x9B
        && isZero(bytes, 4, 12)) {
      return lowestFourBytes(bytes);
    }
    // 6to4 (2002::/16, RFC 3056): the gateway IPv4 is the 32 bits after the prefix. 2002::/16 is
    // reserved exclusively for 6to4, so no legitimate non-6to4 host occupies it.
    if ((bytes[0] & 0xFF) == 0x20 && (bytes[1] & 0xFF) == 0x02) {
      return new byte[] {bytes[2], bytes[3], bytes[4], bytes[5]};
    }
    // ISATAP interface identifier (RFC 5214): the low 64 bits are 00:00:5e:fe:a.b.c.d or
    // 02:00:5e:fe:a.b.c.d, embedding the IPv4 in the low 32 bits regardless of the /64 prefix.
    if ((bytes[8] & 0xFD) == 0x00
        && bytes[9] == 0x00
        && (bytes[10] & 0xFF) == 0x5E
        && (bytes[11] & 0xFF) == 0xFE) {
      return lowestFourBytes(bytes);
    }
    return null;
  }

  private static byte[] lowestFourBytes(byte[] bytes) {
    return new byte[] {bytes[12], bytes[13], bytes[14], bytes[15]};
  }

  private static boolean isZero(byte[] bytes, int fromInclusive, int toExclusive) {
    for (int i = fromInclusive; i < toExclusive; i++) {
      if (bytes[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private static boolean isUnsafeIpv4Address(byte[] bytes) {
    if (bytes.length != 4) {
      return false;
    }
    int b0 = bytes[0] & 0xFF;
    int b1 = bytes[1] & 0xFF;
    int b2 = bytes[2] & 0xFF;
    int b3 = bytes[3] & 0xFF;

    // 0.0.0.0/8 "this network" (RFC 1122). isAnyLocalAddress only covers the single 0.0.0.0.
    if (b0 == 0) {
      return true;
    }
    // 100.64.0.0/10 carrier-grade NAT / shared address space (RFC 6598). This range also covers
    // the Alibaba Cloud metadata endpoint 100.100.100.200. Not caught by isSiteLocalAddress.
    if (b0 == 100 && (b1 & 0xC0) == 0x40) {
      return true;
    }
    // 192.0.0.192 Oracle Cloud Infrastructure instance-metadata endpoint.
    if (b0 == 192 && b1 == 0 && b2 == 0 && b3 == 192) {
      return true;
    }
    // 255.255.255.255 limited broadcast (RFC 919).
    return b0 == 255 && b1 == 255 && b2 == 255 && b3 == 255;
  }

  private static boolean isIpv6UniqueLocalAddress(byte[] bytes) {
    return bytes.length == 16 && ((bytes[0] & 0xFE) == 0xFC);
  }
}
