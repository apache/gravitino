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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClientAddressResolver {

  private static final ClientAddressResolver TRUST_PRIVATE_V4 =
      new ClientAddressResolver(ImmutableList.of("10.0.0.0/8", "192.168.1.10"));

  @Test
  public void testNoTrustedProxiesIgnoresForwardedFor() {
    HttpServletRequest request = request("10.0.0.1", "203.0.113.5");
    Assertions.assertEquals("10.0.0.1", ClientAddressResolver.noTrustedProxies().resolve(request));
    Assertions.assertEquals(
        "10.0.0.1", new ClientAddressResolver(Collections.emptyList()).resolve(request));
  }

  @Test
  public void testUntrustedPeerWithSpoofedForwardedForIsIgnored() {
    // A client connecting directly cannot choose the recorded address by sending the header.
    HttpServletRequest request = request("198.51.100.9", "10.0.0.1, 203.0.113.5");
    Assertions.assertEquals("198.51.100.9", TRUST_PRIVATE_V4.resolve(request));
  }

  @Test
  public void testTrustedPeerWithoutForwardedForReturnsPeer() {
    Assertions.assertEquals("10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1")));
  }

  @Test
  public void testSingleTrustedProxy() {
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("192.168.1.10", "203.0.113.5")));
  }

  @Test
  public void testTrustedProxyChainReturnsRightMostUntrusted() {
    // The client prepended a forged 198.51.100.7; the real client is the entry the first trusted
    // proxy appended.
    HttpServletRequest request =
        request("10.0.0.1", "198.51.100.7, 203.0.113.5, 10.2.2.2, 192.168.1.10");
    Assertions.assertEquals("203.0.113.5", TRUST_PRIVATE_V4.resolve(request));
  }

  @Test
  public void testCidrMatching() {
    ClientAddressResolver resolver = new ClientAddressResolver(ImmutableList.of("172.16.0.0/12"));
    Assertions.assertEquals(
        "203.0.113.5", resolver.resolve(request("172.31.255.254", "203.0.113.5")));
    Assertions.assertEquals("172.32.0.1", resolver.resolve(request("172.32.0.1", "203.0.113.5")));
    Assertions.assertEquals(
        "172.15.255.255", resolver.resolve(request("172.15.255.255", "203.0.113.5")));
  }

  @Test
  public void testNonOctetAlignedCidr() {
    ClientAddressResolver resolver = new ClientAddressResolver(ImmutableList.of("10.1.2.64/27"));
    Assertions.assertEquals("203.0.113.5", resolver.resolve(request("10.1.2.95", "203.0.113.5")));
    Assertions.assertEquals("10.1.2.96", resolver.resolve(request("10.1.2.96", "203.0.113.5")));
  }

  @Test
  public void testZeroPrefixTrustsEverything() {
    ClientAddressResolver resolver = new ClientAddressResolver(ImmutableList.of("0.0.0.0/0"));
    // Every entry is trusted, so the left-most one is returned.
    Assertions.assertEquals(
        "198.51.100.7", resolver.resolve(request("203.0.113.1", "198.51.100.7, 203.0.113.5")));
  }

  @Test
  public void testAllEntriesTrustedReturnsLeftMost() {
    HttpServletRequest request = request("10.0.0.1", "10.3.3.3, 10.2.2.2");
    Assertions.assertEquals("10.3.3.3", TRUST_PRIVATE_V4.resolve(request));
  }

  @Test
  public void testIpv6() {
    ClientAddressResolver resolver = new ClientAddressResolver(ImmutableList.of("fd00::/8", "::1"));
    // Jetty reports an IPv6 peer in brackets and uncompressed.
    Assertions.assertEquals(
        "2001:db8::1",
        resolver.resolve(request("[fd12:3456:0:0:0:0:0:1]", "2001:db8:0:0:0:0:0:1")));
    Assertions.assertEquals(
        "2001:db8::1", resolver.resolve(request("[0:0:0:0:0:0:0:1]", "2001:db8::1, fd00::2")));
    Assertions.assertEquals(
        "fe80::1", resolver.resolve(request("[fe80:0:0:0:0:0:0:1]", "2001:db8::1")));
    // The unbracketed form is accepted too.
    Assertions.assertEquals("2001:db8::1", resolver.resolve(request("::1", "2001:db8::1")));
  }

  @Test
  public void testPeerAddressIsCanonical() {
    // Without trusted proxies too, so the audit log never mixes forms of the same address.
    ClientAddressResolver none = ClientAddressResolver.noTrustedProxies();
    Assertions.assertEquals("::1", none.resolve(request("[0:0:0:0:0:0:0:1]", "203.0.113.5")));
    Assertions.assertEquals("::1", none.resolve(request("0:0:0:0:0:0:0:1")));
    Assertions.assertEquals("fe80::1", none.resolve(request("[fe80:0:0:0:0:0:0:1%1]")));
    Assertions.assertEquals("10.0.0.1", none.resolve(request("::ffff:10.0.0.1")));
    Assertions.assertEquals("10.0.0.1", none.resolve(request("10.0.0.1")));
    // Untrusted peer and trusted peer without X-Forwarded-For.
    Assertions.assertEquals(
        "2001:db8::5", TRUST_PRIVATE_V4.resolve(request("[2001:db8:0:0:0:0:0:5]", "1.2.3.4")));
    ClientAddressResolver trustLoopback = new ClientAddressResolver(ImmutableList.of("::1"));
    Assertions.assertEquals("::1", trustLoopback.resolve(request("[0:0:0:0:0:0:0:1]")));
    Assertions.assertEquals(
        "::1", trustLoopback.resolve(request("[0:0:0:0:0:0:0:1]", "203.0.113.5, unknown")));
  }

  @Test
  public void testLinkLocalPeerIsNeverTrusted() {
    // fe80::1%1 and fe80::1%2 can be different hosts on different links (RFC 4007, section 12),
    // so a link-local peer must not inherit trust from a wider range that covers it.
    ClientAddressResolver allIpv6 = new ClientAddressResolver(ImmutableList.of("::/0"));
    Assertions.assertEquals(
        "fe80::1", allIpv6.resolve(request("[fe80:0:0:0:0:0:0:1%1]", "203.0.113.77")));
    Assertions.assertEquals(
        "fe80::1", allIpv6.resolve(request("[fe80:0:0:0:0:0:0:1%2]", "203.0.113.77")));
    Assertions.assertEquals("fe80::1", allIpv6.resolve(request("fe80::1", "203.0.113.77")));
    // Other IPv6 peers in the same range are still trusted.
    Assertions.assertEquals(
        "203.0.113.77", allIpv6.resolve(request("[fd00:0:0:0:0:0:0:1]", "203.0.113.77")));
    // A link-local entry in the header is reported as the client, never skipped as a proxy.
    Assertions.assertEquals(
        "fe80::5", allIpv6.resolve(request("[fd00:0:0:0:0:0:0:1]", "203.0.113.77, fe80::5")));
  }

  @Test
  public void testLinkLocalTrustedProxyIsRejected() {
    for (String invalid : Arrays.asList("fe80::1", "fe80::/10", "febf::1/128", "fe80::/9")) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new ClientAddressResolver(ImmutableList.of(invalid)),
              "Expected rejection of " + invalid);
      Assertions.assertTrue(e.getMessage().contains("link-local"), e.getMessage());
    }
    // A zone ID is not accepted either.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ClientAddressResolver(ImmutableList.of("fe80::1%1")));
  }

  @Test
  public void testIpv4MappedIpv6() {
    // An IPv4 proxy entry matches the IPv4-mapped IPv6 form a dual-stack socket reports.
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("::ffff:10.0.0.1", "203.0.113.5")));
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("[::ffff:10.0.0.1]", "203.0.113.5")));

    // An IPv4-mapped IPv6 CIDR matches the plain IPv4 form.
    ClientAddressResolver mapped =
        new ClientAddressResolver(ImmutableList.of("::ffff:10.0.0.0/104"));
    Assertions.assertEquals("203.0.113.5", mapped.resolve(request("10.9.8.7", "203.0.113.5")));
    Assertions.assertEquals("11.0.0.1", mapped.resolve(request("11.0.0.1", "203.0.113.5")));

    // A mapped entry in the header is returned in canonical IPv4 form.
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "::ffff:203.0.113.5")));
  }

  @Test
  public void testIpv4AndIpv6RangesDoNotOverlap() {
    // 10.0.0.0/8 must not be read as an IPv6 /8 that would cover unrelated addresses.
    Assertions.assertEquals("::a00:1", TRUST_PRIVATE_V4.resolve(request("::a00:1", "203.0.113.5")));
  }

  @Test
  public void testMultipleHeadersAreCombinedInOrder() {
    HttpServletRequest request =
        request("10.0.0.1", Arrays.asList("198.51.100.7, 203.0.113.5", "10.2.2.2"));
    Assertions.assertEquals("203.0.113.5", TRUST_PRIVATE_V4.resolve(request));

    HttpServletRequest oneEntryPerHeader =
        request("10.0.0.1", Arrays.asList("203.0.113.5", "10.2.2.2"));
    Assertions.assertEquals("203.0.113.5", TRUST_PRIVATE_V4.resolve(oneEntryPerHeader));
  }

  @Test
  public void testPortsAndBracketsAreStripped() {
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5:4711")));
    Assertions.assertEquals(
        "2001:db8::1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "[2001:db8::1]:4711")));
    Assertions.assertEquals(
        "2001:db8::1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "[2001:db8::1]")));
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5, 10.2.2.2:80")));
  }

  @Test
  public void testMalformedEntryStopsAtLastTrustedHop() {
    // Malformed right after the peer: fall back to the peer itself.
    Assertions.assertEquals(
        "10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5, not-an-ip")));
    // Malformed behind a trusted hop: return that hop, never anything left of the bad entry.
    Assertions.assertEquals(
        "10.2.2.2",
        TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5, unknown, 10.2.2.2")));
    Assertions.assertEquals(
        "10.2.2.2", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5, bad, 10.2.2.2")));
    Assertions.assertEquals(
        "10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5:http")));
    Assertions.assertEquals(
        "10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "[2001:db8::1")));
    Assertions.assertEquals(
        "10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "[2001:db8::1]x")));
    Assertions.assertEquals("10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "UNKNOWN")));
    // Host names are never resolved.
    Assertions.assertEquals("10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "localhost")));
  }

  @Test
  public void testEmptyListElementsAreIgnored() {
    // RFC 7230 section 7: empty elements in a comma-separated list are ignored.
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5, , 10.2.2.2")));
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5,")));
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", ",203.0.113.5,,")));
    Assertions.assertEquals(
        "203.0.113.5", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", "203.0.113.5", " ", "")));
    Assertions.assertEquals("10.0.0.1", TRUST_PRIVATE_V4.resolve(request("10.0.0.1", " , ")));
  }

  @Test
  public void testUnparsablePeerIsNotTrusted() {
    Assertions.assertEquals(
        "some-host", TRUST_PRIVATE_V4.resolve(request("some-host", "203.0.113.5")));
    Assertions.assertNull(TRUST_PRIVATE_V4.resolve(request(null, "203.0.113.5")));
    Assertions.assertEquals("[", TRUST_PRIVATE_V4.resolve(request("[", "203.0.113.5")));
    Assertions.assertEquals("[]", TRUST_PRIVATE_V4.resolve(request("[]", "203.0.113.5")));
  }

  @Test
  public void testNullHeaderEnumerationIsTreatedAsAbsent() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteAddr()).thenReturn("10.0.0.1");
    when(request.getHeaders(ClientAddressResolver.X_FORWARDED_FOR)).thenReturn(null);
    Assertions.assertEquals("10.0.0.1", TRUST_PRIVATE_V4.resolve(request));
  }

  @Test
  public void testInvalidTrustedProxyIsRejected() {
    for (String invalid :
        Arrays.asList(
            "proxy.example.com",
            "10.0.0.256",
            "10.0.0.0/33",
            "10.0.0.0/-1",
            "fd00::/129",
            "10.0.0.0/8/8",
            "::ffff:10.0.0.0/129",
            "fe80::1%eth0")) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new ClientAddressResolver(ImmutableList.of(invalid)),
              "Expected rejection of " + invalid);
      Assertions.assertTrue(e.getMessage().contains(invalid), e.getMessage());
    }
  }

  @Test
  public void testOutOfRangePrefixReportsRange() {
    for (String invalid :
        Arrays.asList("10.0.0.0/0128", "10.0.0.0/33", "fd00::/1290", "10.0.0.0/99999999999")) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> new ClientAddressResolver(ImmutableList.of(invalid)));
      Assertions.assertTrue(e.getMessage().contains("must be between 0 and"), e.getMessage());
    }
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new ClientAddressResolver(ImmutableList.of("10.0.0.0/abc")));
    Assertions.assertTrue(e.getMessage().contains("must be a number"), e.getMessage());
  }

  @Test
  public void testZeroPaddedPrefixIsAccepted() {
    // The value decides, not the digit count.
    for (String range : Arrays.asList("10.0.0.0/024", "10.0.0.0/0024")) {
      ClientAddressResolver resolver = new ClientAddressResolver(ImmutableList.of(range));
      Assertions.assertEquals("203.0.113.5", resolver.resolve(request("10.0.0.5", "203.0.113.5")));
      Assertions.assertEquals("10.0.1.5", resolver.resolve(request("10.0.1.5", "203.0.113.5")));
    }
  }

  @Test
  public void testBlankTrustedProxyEntriesAreIgnored() {
    ClientAddressResolver resolver =
        new ClientAddressResolver(Arrays.asList(" ", "", " 10.0.0.1 "));
    Assertions.assertEquals("203.0.113.5", resolver.resolve(request("10.0.0.1", "203.0.113.5")));
  }

  private static HttpServletRequest request(String remoteAddr, String... forwardedFor) {
    return request(remoteAddr, Arrays.asList(forwardedFor));
  }

  private static HttpServletRequest request(String remoteAddr, List<String> forwardedFor) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteAddr()).thenReturn(remoteAddr);
    when(request.getHeaders(ClientAddressResolver.X_FORWARDED_FOR))
        .thenReturn(Collections.enumeration(forwardedFor));
    return request;
  }
}
