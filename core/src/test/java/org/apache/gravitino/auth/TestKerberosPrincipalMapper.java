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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestKerberosPrincipalMapper {

  private KerberosPrincipalMapper mapper;

  @BeforeEach
  public void setUp() {
    mapper = new KerberosPrincipalMapper();
  }

  @Test
  public void testUsernameOnly() {
    KerberosPrincipal principal = (KerberosPrincipal) mapper.map("john");

    assertEquals("john", principal.getName());
    assertEquals("john", principal.getPrimaryWithInstance());
    assertFalse(principal.getInstance().isPresent());
    assertFalse(principal.getRealm().isPresent());
    assertEquals("john", principal.getFullPrincipal());
  }

  @Test
  public void testUsernameWithRealm() {
    KerberosPrincipal principal = (KerberosPrincipal) mapper.map("john@EXAMPLE.COM");

    assertEquals("john", principal.getName());
    assertEquals("john", principal.getPrimaryWithInstance());
    assertFalse(principal.getInstance().isPresent());
    assertTrue(principal.getRealm().isPresent());
    assertEquals("EXAMPLE.COM", principal.getRealm().get());
    assertEquals("john@EXAMPLE.COM", principal.getFullPrincipal());
  }

  @Test
  public void testServicePrincipalWithInstanceAndRealm() {
    KerberosPrincipal principal =
        (KerberosPrincipal) mapper.map("HTTP/server.example.com@EXAMPLE.COM");

    assertEquals("HTTP", principal.getName());
    assertEquals("HTTP/server.example.com", principal.getPrimaryWithInstance());
    assertTrue(principal.getInstance().isPresent());
    assertEquals("server.example.com", principal.getInstance().get());
    assertTrue(principal.getRealm().isPresent());
    assertEquals("EXAMPLE.COM", principal.getRealm().get());
    assertEquals("HTTP/server.example.com@EXAMPLE.COM", principal.getFullPrincipal());
  }

  @Test
  public void testUsernameWithInstance() {
    KerberosPrincipal principal = (KerberosPrincipal) mapper.map("user/admin");

    assertEquals("user", principal.getName());
    assertEquals("user/admin", principal.getPrimaryWithInstance());
    assertTrue(principal.getInstance().isPresent());
    assertEquals("admin", principal.getInstance().get());
    assertFalse(principal.getRealm().isPresent());
    assertEquals("user/admin", principal.getFullPrincipal());
  }

  @Test
  public void testNullPrincipal() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          mapper.map(null);
        });
  }

  @Test
  public void testEmptyPrincipal() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          mapper.map("");
        });
  }

  @Test
  public void testEmptyUsername() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          mapper.map("@EXAMPLE.COM");
        });
  }

  @Test
  public void testInvalidFormatRealmBeforeSlash() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          mapper.map("user@EXAMPLE.COM/instance");
        });
  }

  @Test
  public void testConstructorWithNullUsername() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new KerberosPrincipal(null, null, null);
        });
  }

  @Test
  public void testConstructorWithEmptyUsername() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new KerberosPrincipal("", null, null);
        });
  }

  @Test
  public void testEqualsAndHashCode() {
    KerberosPrincipal principal1 = new KerberosPrincipal("john", "admin", "EXAMPLE.COM");
    KerberosPrincipal principal2 = new KerberosPrincipal("john", "admin", "EXAMPLE.COM");
    KerberosPrincipal principal3 = new KerberosPrincipal("john", null, "EXAMPLE.COM");

    assertEquals(principal1, principal2);
    assertEquals(principal1.hashCode(), principal2.hashCode());
    assertFalse(principal1.equals(principal3));
  }

  @Test
  public void testToString() {
    KerberosPrincipal principal =
        (KerberosPrincipal) mapper.map("HTTP/server.example.com@EXAMPLE.COM");
    String toString = principal.toString();

    assertNotNull(toString);
    assertTrue(toString.contains("HTTP/server.example.com@EXAMPLE.COM"));
  }

  @Test
  public void testConstructorDirectly() {
    KerberosPrincipal principal = new KerberosPrincipal("john", "admin", "EXAMPLE.COM");

    assertEquals("john", principal.getName());
    assertEquals("john/admin", principal.getPrimaryWithInstance());
    assertEquals("admin", principal.getInstance().get());
    assertEquals("EXAMPLE.COM", principal.getRealm().get());
    assertEquals("john/admin@EXAMPLE.COM", principal.getFullPrincipal());
  }
}
