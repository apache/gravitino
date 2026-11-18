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

import java.net.InetAddress;
import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRemoteUriValidator {
  private static final String BLOCK_UNSAFE_ADDRESS_CONFIG = "test.block-unsafe-address";

  @Test
  public void testResolveAndValidateReturnsAddressForSafeHost() throws Exception {
    // A public literal IP resolves to itself (no DNS) and is safe, so it must be returned for the
    // caller to pin the subsequent download to.
    InetAddress address =
        RemoteUriValidator.resolveAndValidate(
            new URI("http://8.8.8.8/file"), BLOCK_UNSAFE_ADDRESS_CONFIG);
    Assertions.assertEquals("8.8.8.8", address.getHostAddress());
  }

  @Test
  public void testResolveAndValidateRejectsUnsafeHost() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteUriValidator.resolveAndValidate(
                    new URI("http://127.0.0.1/file"), BLOCK_UNSAFE_ADDRESS_CONFIG));
    Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(exception.getMessage().contains(BLOCK_UNSAFE_ADDRESS_CONFIG));
  }

  @Test
  public void testRejectLocalAddressesByDefault() {
    // Loopback, link-local (incl. the AWS/cloud metadata 169.254.169.254), RFC 1918 private ranges,
    // Alibaba/Oracle metadata, CGNAT, broadcast, "this network", multicast, and IPv6 unique-local
    // must all be rejected.
    String[] unsafeHosts = {
      "127.0.0.1",
      "localhost",
      "169.254.169.254",
      "10.0.0.1",
      "172.16.0.1",
      "192.168.0.1",
      "100.100.100.200",
      "100.64.0.1",
      "192.0.0.192",
      "255.255.255.255",
      "0.0.0.1",
      "224.0.0.1",
      "[ff02::1]",
      "[fd00::1]",
      // IPv4-compatible, NAT64, 6to4 and ISATAP IPv6 forms that embed a blocked IPv4 are rejected.
      "[::127.0.0.1]",
      "[::169.254.169.254]",
      "[64:ff9b::169.254.169.254]",
      "[2002:a9fe:a9fe::]",
      "[2001:db8::5efe:169.254.169.254]"
    };
    for (String host : unsafeHosts) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  RemoteUriValidator.resolveAndValidate(
                      new URI("http://" + host + "/"), BLOCK_UNSAFE_ADDRESS_CONFIG),
              "Expected " + host + " to be rejected");
      Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
      Assertions.assertTrue(exception.getMessage().contains(BLOCK_UNSAFE_ADDRESS_CONFIG));
    }
  }

  @Test
  public void testResolveAndValidateAllowsGlobalIpv6() throws Exception {
    // A normal global IPv6 host (no embedded blocked IPv4) must not be over-blocked by the
    // embedded-IPv4 re-classification.
    InetAddress address =
        RemoteUriValidator.resolveAndValidate(
            new URI("http://[2001:4860:4860::8888]/file"), BLOCK_UNSAFE_ADDRESS_CONFIG);
    Assertions.assertNotNull(address);
  }

  @Test
  public void testRejectMissingHost() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RemoteUriValidator.resolveAndValidate(new URI("file:///tmp/x"), "hint"));
  }
}
