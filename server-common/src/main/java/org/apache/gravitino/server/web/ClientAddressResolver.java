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

package org.apache.gravitino.server.web;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;

/**
 * Resolves the address of the client that sent an HTTP request, honoring {@code X-Forwarded-For}
 * only when the request reached the server through a configured trusted proxy.
 *
 * <p>Each trusted proxy is an IP literal or a CIDR block, IPv4 or IPv6. Entries are parsed with
 * {@link InetAddresses#forString(String)}, which never performs a DNS lookup. IPv4 and IPv4-mapped
 * IPv6 forms of the same address match each other.
 *
 * <p>IPv6 link-local addresses ({@code fe80::/10}) are never trusted. The same link-local address
 * can name different hosts on different links, told apart only by the zone ID (RFC 4007, section
 * 12), and matching zones reliably is not supported. A trusted proxy entry inside {@code fe80::/10}
 * fails at construction, and a link-local peer is not trusted even when a wider entry such as
 * {@code ::/0} covers it.
 *
 * <p>The resolution rules are:
 *
 * <ol>
 *   <li>If the TCP peer ({@link HttpServletRequest#getRemoteAddr()}) is not a trusted proxy, return
 *       it and ignore {@code X-Forwarded-For} entirely: any caller can set that header, so it says
 *       nothing about who the caller is unless a proxy we trust wrote it.
 *   <li>Otherwise combine every {@code X-Forwarded-For} header in the order received and walk the
 *       entries from right to left, skipping trusted proxies. The first untrusted entry is the
 *       client. Entries left of it were supplied by the client and are not trusted.
 *   <li>If every entry is a trusted proxy, return the left-most one.
 *   <li>Empty list elements, such as the one in {@code 203.0.113.5, , 10.2.2.2}, are ignored.
 *   <li>If an entry is malformed (not an IP literal, or {@code unknown}), stop and return the last
 *       trusted hop seen so far, which may be the TCP peer itself.
 * </ol>
 *
 * <p>Ports and IPv6 brackets ({@code 203.0.113.5:4711}, {@code [2001:db8::1]:4711}) are stripped
 * from {@code X-Forwarded-For} entries. Every returned address, the TCP peer included, is in
 * canonical form: IPv6 compressed as in RFC 5952 and without brackets or a zone ID (Jetty reports
 * {@code [0:0:0:0:0:0:0:1]}, this returns {@code ::1}), and an IPv4-mapped IPv6 address as plain
 * IPv4. A peer address that is not an IP literal is returned unchanged.
 *
 * <p>With no trusted proxies configured (the default), {@code X-Forwarded-For} is never honored and
 * the TCP peer address is always returned.
 */
public final class ClientAddressResolver {

  /** The name of the de-facto standard header that reverse proxies append the client address to. */
  public static final String X_FORWARDED_FOR = "X-Forwarded-For";

  private static final String UNKNOWN = "unknown";
  private static final int IPV6_BITS = 128;
  private static final int IPV4_BITS = 32;
  private static final int IPV4_MAPPED_PREFIX_BITS = IPV6_BITS - IPV4_BITS;

  private static final ClientAddressResolver NO_TRUSTED_PROXIES =
      new ClientAddressResolver(Collections.emptyList());

  private final List<IpRange> trustedProxies;

  /**
   * Constructs a resolver that trusts the given proxies.
   *
   * @param trustedProxies IP literals or CIDR blocks, for example {@code 10.0.0.1}, {@code
   *     10.0.0.0/8} or {@code fd00::/8}; blank entries are ignored.
   * @throws IllegalArgumentException if an entry is not a valid IP literal or CIDR block.
   */
  public ClientAddressResolver(List<String> trustedProxies) {
    ImmutableList.Builder<IpRange> ranges = ImmutableList.builder();
    for (String proxy : trustedProxies) {
      if (StringUtils.isNotBlank(proxy)) {
        ranges.add(IpRange.parse(proxy.trim()));
      }
    }
    this.trustedProxies = ranges.build();
  }

  /**
   * Returns a resolver with no trusted proxies, which always returns the TCP peer address and never
   * honors {@code X-Forwarded-For}.
   *
   * @return a resolver with no trusted proxies
   */
  public static ClientAddressResolver noTrustedProxies() {
    return NO_TRUSTED_PROXIES;
  }

  /**
   * Resolves the client address of the given request. See the class doc for the rules.
   *
   * @param request the HTTP request
   * @return the client address in canonical form; the TCP peer address when {@code X-Forwarded-For}
   *     is not honored; {@code null} only when the container reports no peer address and {@code
   *     X-Forwarded-For} is not honored
   */
  @Nullable
  public String resolve(HttpServletRequest request) {
    String remoteAddr = request.getRemoteAddr();
    Optional<InetAddress> peer = parseAddress(remoteAddr);
    String peerAddress = peer.map(InetAddresses::toAddrString).orElse(remoteAddr);
    if (trustedProxies.isEmpty() || !isTrusted(peer)) {
      return peerAddress;
    }

    List<String> entries = forwardedForEntries(request);
    String lastTrustedHop = peerAddress;
    for (int i = entries.size() - 1; i >= 0; i--) {
      Optional<InetAddress> address = parseForwardedEntry(entries.get(i));
      if (!address.isPresent()) {
        return lastTrustedHop;
      }
      String canonical = InetAddresses.toAddrString(address.get());
      if (!isTrusted(address)) {
        return canonical;
      }
      lastTrustedHop = canonical;
    }
    return lastTrustedHop;
  }

  private boolean isTrusted(Optional<InetAddress> address) {
    if (!address.isPresent() || isIpv6LinkLocal(address.get())) {
      return false;
    }
    byte[] bytes = toIpv6Bytes(address.get());
    return trustedProxies.stream().anyMatch(range -> range.contains(bytes));
  }

  private static List<String> forwardedForEntries(HttpServletRequest request) {
    ImmutableList.Builder<String> entries = ImmutableList.builder();
    Enumeration<String> headers = request.getHeaders(X_FORWARDED_FOR);
    while (headers != null && headers.hasMoreElements()) {
      String header = headers.nextElement();
      if (header == null) {
        continue;
      }
      for (String entry : header.split(",")) {
        String trimmed = entry.trim();
        // RFC 7230 section 7: empty list elements must be ignored, not treated as malformed.
        if (!trimmed.isEmpty()) {
          entries.add(trimmed);
        }
      }
    }
    return entries.build();
  }

  /**
   * Parses one {@code X-Forwarded-For} entry, stripping an optional port and IPv6 brackets. Returns
   * empty for anything that is not an IP literal, including {@code unknown} and obfuscated
   * identifiers.
   */
  private static Optional<InetAddress> parseForwardedEntry(String entry) {
    if (UNKNOWN.equalsIgnoreCase(entry)) {
      return Optional.empty();
    }
    String host;
    if (entry.startsWith("[")) {
      int closing = entry.indexOf(']');
      if (closing < 0) {
        return Optional.empty();
      }
      String rest = entry.substring(closing + 1);
      if (!rest.isEmpty() && !isPortSuffix(rest)) {
        return Optional.empty();
      }
      host = entry.substring(1, closing);
    } else if (entry.indexOf(':') >= 0 && entry.indexOf(':') == entry.lastIndexOf(':')) {
      // Exactly one colon: an IPv4 address with a port. A bare IPv6 address has at least two.
      int colon = entry.indexOf(':');
      if (!isPortSuffix(entry.substring(colon))) {
        return Optional.empty();
      }
      host = entry.substring(0, colon);
    } else {
      host = entry;
    }
    return parseAddress(host);
  }

  private static boolean isPortSuffix(String suffix) {
    return suffix.length() > 1
        && suffix.charAt(0) == ':'
        && StringUtils.isNumeric(suffix.substring(1));
  }

  private static Optional<InetAddress> parseAddress(@Nullable String text) {
    if (StringUtils.isBlank(text)) {
      return Optional.empty();
    }
    String host = text.trim();
    // Jetty reports an IPv6 peer in brackets, such as "[0:0:0:0:0:0:0:1]".
    if (host.length() > 1 && host.charAt(0) == '[' && host.charAt(host.length() - 1) == ']') {
      host = host.substring(1, host.length() - 1);
    }
    // Drop an IPv6 zone ID, such as "fe80::1%eth0". Only link-local addresses carry one, and those
    // are never trusted, so dropping it cannot make an address trusted.
    int zone = host.indexOf('%');
    if (zone >= 0) {
      host = host.substring(0, zone);
    }
    if (!InetAddresses.isInetAddress(host)) {
      return Optional.empty();
    }
    return Optional.of(InetAddresses.forString(host));
  }

  private static boolean isIpv6LinkLocal(InetAddress address) {
    return address instanceof Inet6Address && address.isLinkLocalAddress();
  }

  /** Returns the 16-byte IPv6 form of an address, mapping IPv4 to {@code ::ffff:a.b.c.d}. */
  private static byte[] toIpv6Bytes(InetAddress address) {
    byte[] raw = address.getAddress();
    if (raw.length == 16) {
      return raw;
    }
    byte[] mapped = new byte[16];
    mapped[10] = (byte) 0xff;
    mapped[11] = (byte) 0xff;
    System.arraycopy(raw, 0, mapped, 12, 4);
    return mapped;
  }

  /** An IP literal or CIDR block, stored in 16-byte IPv6 form so IPv4 and IPv6 compare alike. */
  private static final class IpRange {
    private final byte[] network;
    private final int prefixBits;

    private IpRange(byte[] network, int prefixBits) {
      this.network = network;
      this.prefixBits = prefixBits;
    }

    static IpRange parse(String text) {
      int slash = text.indexOf('/');
      String addressPart = slash >= 0 ? text.substring(0, slash).trim() : text;
      if (addressPart.indexOf('%') >= 0 || !InetAddresses.isInetAddress(addressPart)) {
        throw invalid(text, "not an IP literal or CIDR block");
      }
      InetAddress address = InetAddresses.forString(addressPart);
      if (isIpv6LinkLocal(address)) {
        throw invalid(
            text,
            "IPv6 link-local addresses cannot be trusted proxies, because the same address can"
                + " name different hosts on different links");
      }
      // Java turns an IPv4-mapped IPv6 literal into an Inet4Address, so look at how the address
      // was written to decide whether a prefix length counts IPv4 or IPv6 bits.
      boolean writtenAsIpv4 = address instanceof Inet4Address && addressPart.indexOf(':') < 0;
      int maxBits = writtenAsIpv4 ? IPV4_BITS : IPV6_BITS;

      int prefix = maxBits;
      if (slash >= 0) {
        String prefixPart = text.substring(slash + 1).trim();
        if (!StringUtils.isNumeric(prefixPart)) {
          throw invalid(text, "the prefix length must be a number");
        }
        int parsed;
        try {
          parsed = Integer.parseInt(prefixPart);
        } catch (NumberFormatException overflow) {
          throw invalid(text, "the prefix length must be between 0 and " + maxBits);
        }
        if (parsed > maxBits) {
          throw invalid(text, "the prefix length must be between 0 and " + maxBits);
        }
        prefix = parsed;
      }
      int prefixBits = writtenAsIpv4 ? prefix + IPV4_MAPPED_PREFIX_BITS : prefix;
      return new IpRange(mask(toIpv6Bytes(address), prefixBits), prefixBits);
    }

    boolean contains(byte[] ipv6Bytes) {
      return Arrays.equals(network, mask(ipv6Bytes, prefixBits));
    }

    private static byte[] mask(byte[] bytes, int prefixBits) {
      byte[] masked = bytes.clone();
      for (int i = 0; i < masked.length; i++) {
        int bitsInByte = Math.max(0, Math.min(8, prefixBits - i * 8));
        masked[i] &= (byte) (0xff << (8 - bitsInByte));
      }
      return masked;
    }

    private static IllegalArgumentException invalid(String text, String reason) {
      return new IllegalArgumentException(
          String.format("Invalid trusted proxy '%s': %s", text, reason));
    }
  }
}
