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

import org.apache.gravitino.UserPrincipal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPrincipalUtils {

  @Test
  public void testNormal() throws Exception {
    UserPrincipal principal = new UserPrincipal("testNormal");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Assertions.assertEquals("testNormal", PrincipalUtils.getCurrentPrincipal().getName());
          return null;
        });
  }

  @Test
  public void testThread() throws Exception {
    UserPrincipal principal = new UserPrincipal("testThread");
    PrincipalUtils.doAs(
        principal,
        () -> {
          Thread thread =
              new Thread(
                  () ->
                      Assertions.assertEquals(
                          "testThread", PrincipalUtils.getCurrentPrincipal().getName()));
          thread.start();
          thread.join();
          return null;
        });
  }

  // Tests for applyUserMappingPattern

  @Test
  public void testApplyUserMappingPatternWithDefaultPattern() {
    // Default pattern (.*) should match everything and return the entire principal
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", "(.*)");
    Assertions.assertEquals("user@example.com", result);

    result = PrincipalUtils.applyUserMappingPattern("admin", "(.*)");
    Assertions.assertEquals("admin", result);
  }

  @Test
  public void testApplyUserMappingPatternExtractEmailLocalPart() {
    // Extract local part from email: user@example.com -> user
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", "([^@]+)@.*");
    Assertions.assertEquals("user", result);

    result = PrincipalUtils.applyUserMappingPattern("john.doe@company.org", "([^@]+)@.*");
    Assertions.assertEquals("john.doe", result);
  }

  @Test
  public void testApplyUserMappingPatternExtractDomain() {
    // Extract domain from email: user@example.com -> example.com
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", "[^@]+@(.*)");
    Assertions.assertEquals("example.com", result);

    result = PrincipalUtils.applyUserMappingPattern("admin@internal.company.com", "[^@]+@(.*)");
    Assertions.assertEquals("internal.company.com", result);
  }

  @Test
  public void testApplyUserMappingPatternNoMatch() {
    // Pattern doesn't match, should return original principal
    String result = PrincipalUtils.applyUserMappingPattern("plainuser", "([^@]+)@.*");
    Assertions.assertEquals("plainuser", result);

    result = PrincipalUtils.applyUserMappingPattern("no-at-sign", "(.+)@(.+)");
    Assertions.assertEquals("no-at-sign", result);
  }

  @Test
  public void testApplyUserMappingPatternWithNoCapturingGroup() {
    // Pattern matches but has no capturing group, should return original principal
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", ".*@example\\.com");
    Assertions.assertEquals("user@example.com", result);
  }

  @Test
  public void testApplyUserMappingPatternWithMultipleCapturingGroups() {
    // Only first capturing group should be used
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", "([^@]+)@(.+\\..+)");
    Assertions.assertEquals("user", result);
  }

  @Test
  public void testApplyUserMappingPatternWithNullPrincipal() {
    // Null principal should return null
    String result = PrincipalUtils.applyUserMappingPattern(null, "(.*)");
    Assertions.assertNull(result);
  }

  @Test
  public void testApplyUserMappingPatternWithNullPattern() {
    // Null pattern should return original principal
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", null);
    Assertions.assertEquals("user@example.com", result);
  }

  @Test
  public void testApplyUserMappingPatternWithEmptyPattern() {
    // Empty pattern should return original principal
    String result = PrincipalUtils.applyUserMappingPattern("user@example.com", "");
    Assertions.assertEquals("user@example.com", result);
  }

  @Test
  public void testApplyUserMappingPatternWithInvalidPattern() {
    // Invalid regex pattern should throw IllegalArgumentException
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PrincipalUtils.applyUserMappingPattern("user@example.com", "[invalid("));
  }

  @Test
  public void testApplyUserMappingPatternCaching() {
    // Test that same pattern is reused from cache
    String pattern = "([^@]+)@.*";
    String result1 = PrincipalUtils.applyUserMappingPattern("user1@example.com", pattern);
    String result2 = PrincipalUtils.applyUserMappingPattern("user2@example.com", pattern);
    String result3 = PrincipalUtils.applyUserMappingPattern("user3@example.com", pattern);

    Assertions.assertEquals("user1", result1);
    Assertions.assertEquals("user2", result2);
    Assertions.assertEquals("user3", result3);
  }

  @Test
  public void testApplyUserMappingPatternKerberosStylePrincipal() {
    // Extract username from Kerberos principal: user/host@REALM -> user
    String result =
        PrincipalUtils.applyUserMappingPattern("john/host1.example.com@EXAMPLE.COM", "([^/@]+).*");
    Assertions.assertEquals("john", result);

    // Extract without realm: user@REALM -> user
    result = PrincipalUtils.applyUserMappingPattern("admin@EXAMPLE.COM", "([^@]+)@.*");
    Assertions.assertEquals("admin", result);
  }

  @Test
  public void testApplyUserMappingPatternLDAPStyleDN() {
    // Extract CN from LDAP DN: CN=John Doe,OU=Users,DC=example,DC=com -> John Doe
    String result =
        PrincipalUtils.applyUserMappingPattern(
            "CN=John Doe,OU=Users,DC=example,DC=com", "CN=([^,]+).*");
    Assertions.assertEquals("John Doe", result);
  }

  @Test
  public void testApplyUserMappingPatternWithSpecialCharacters() {
    // Test with special regex characters in principal
    String result =
        PrincipalUtils.applyUserMappingPattern("user.name+tag@example.com", "([^@]+)@.*");
    Assertions.assertEquals("user.name+tag", result);

    // Test with backslash in principal (AD domain format)
    result = PrincipalUtils.applyUserMappingPattern("DOMAIN\\user", ".*\\\\(.+)");
    Assertions.assertEquals("user", result);
  }

  @Test
  public void testApplyUserMappingPatternEmptyCapturingGroup() {
    // If capturing group matches empty string, should return original principal
    String result = PrincipalUtils.applyUserMappingPattern("@example.com", "([^@]*)@.*");
    Assertions.assertEquals("@example.com", result);
  }

  @Test
  public void testApplyUserMappingPatternCaseInsensitive() {
    // Case-insensitive pattern matching
    String result =
        PrincipalUtils.applyUserMappingPattern("User@Example.COM", "(?i)([^@]+)@example\\.com");
    Assertions.assertEquals("User", result);
  }

  @Test
  public void testApplyUserMappingPatternPartialMatch() {
    // find() matches anywhere in string, not just from start
    String result = PrincipalUtils.applyUserMappingPattern("prefix-user@example.com", "([^@]+)@.*");
    Assertions.assertEquals("prefix-user", result);
  }

  @Test
  public void testApplyUserMappingPatternUnicodeCharacters() {
    // Test with Unicode characters in principal
    String result = PrincipalUtils.applyUserMappingPattern("用户@example.com", "([^@]+)@.*");
    Assertions.assertEquals("用户", result);

    result = PrincipalUtils.applyUserMappingPattern("müller@example.de", "([^@]+)@.*");
    Assertions.assertEquals("müller", result);
  }
}
