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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import org.apache.gravitino.UserPrincipal;
import org.junit.jupiter.api.Test;

public class TestRegexPrincipalMapper {

  @Test
  public void testExtractUsernameFromEmail() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("([^@]+)@.*");
    Principal principal = mapper.map("john.doe@company.com");

    assertNotNull(principal);
    assertEquals("john.doe", principal.getName());
    assertTrue(principal instanceof UserPrincipal);
  }

  @Test
  public void testExtractUsernameFromKerberosPrincipal() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("([^/@]+).*");

    // Test user@REALM format
    Principal principal1 = mapper.map("john@EXAMPLE.COM");
    assertNotNull(principal1);
    assertEquals("john", principal1.getName());

    // Test user/instance@REALM format
    Principal principal2 = mapper.map("HTTP/server.example.com@EXAMPLE.COM");
    assertNotNull(principal2);
    assertEquals("HTTP", principal2.getName());
  }

  @Test
  public void testNullPattern() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new RegexPrincipalMapper(null);
        });
  }

  @Test
  public void testEmptyPattern() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new RegexPrincipalMapper("");
        });
  }

  @Test
  public void testNullPrincipal() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("([^@]+)@.*");
    Principal principal = mapper.map(null);

    assertNull(principal);
  }

  @Test
  public void testPatternWithNoCapturingGroup() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper(".*@company.com");
    Principal principal = mapper.map("user@company.com");

    // If pattern matches but has no capturing group, return original
    assertNotNull(principal);
    assertEquals("user@company.com", principal.getName());
  }

  @Test
  public void testPatternDoesNotMatch() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("([^@]+)@company.com");
    Principal principal = mapper.map("user@other.com");

    // If pattern doesn't match, return original
    assertNotNull(principal);
    assertEquals("user@other.com", principal.getName());
  }

  @Test
  public void testInvalidRegexPattern() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new RegexPrincipalMapper("[invalid(regex");
        });
  }

  @Test
  public void testEmptyCapturingGroup() {
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("().*");
    Principal principal = mapper.map("user@company.com");

    // If capturing group is empty, return original principal
    assertNotNull(principal);
    assertEquals("user@company.com", principal.getName());
  }

  @Test
  public void testComplexPattern() {
    // Extract username from Azure AD format: user.name@tenant.onmicrosoft.com
    RegexPrincipalMapper mapper = new RegexPrincipalMapper("([^@]+)@[^.]+\\..*");
    Principal principal = mapper.map("john.doe@mytenant.onmicrosoft.com");

    assertNotNull(principal);
    assertEquals("john.doe", principal.getName());
  }

  @Test
  public void testGetPatternString() {
    String pattern = "([^@]+)@.*";
    RegexPrincipalMapper mapper = new RegexPrincipalMapper(pattern);
    assertEquals(pattern, mapper.getPatternString());
  }
}
