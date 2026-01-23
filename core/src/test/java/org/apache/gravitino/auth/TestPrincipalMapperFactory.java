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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;
import org.apache.gravitino.UserPrincipal;
import org.junit.jupiter.api.Test;

public class TestPrincipalMapperFactory {

  @Test
  public void testCreateRegexMapper() {
    PrincipalMapper mapper = PrincipalMapperFactory.create("regex", "([^@]+)@.*");

    assertNotNull(mapper);
    assertTrue(mapper instanceof RegexPrincipalMapper);

    Principal principal = mapper.map("user@company.com");
    assertEquals("user", principal.getName());
  }

  @Test
  public void testCreateRegexMapperWithDefaultPattern() {
    PrincipalMapper mapper = PrincipalMapperFactory.create("regex", "^(.*)$");

    assertNotNull(mapper);
    assertTrue(mapper instanceof RegexPrincipalMapper);

    Principal principal = mapper.map("user@company.com");
    assertTrue(principal instanceof UserPrincipal);
    assertEquals("user@company.com", principal.getName());
  }

  @Test
  public void testCreateCustomMapperWithInvalidClass() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              PrincipalMapperFactory.create("unknown.InvalidClass", null);
            });
    assertTrue(exception.getMessage().contains("Unknown principal mapper type"));
  }

  @Test
  public void testCreateCustomMapperWithInlineClass() {
    class InlineCustomMapper implements PrincipalMapper {
      @Override
      public Principal map(String principal) {
        return new UserPrincipal("inline:" + principal);
      }
    }
    // Register the class by name in the current classloader
    String className = InlineCustomMapper.class.getName();
    PrincipalMapper mapper = PrincipalMapperFactory.create(className, null);
    assertNotNull(mapper);
    assertTrue(mapper instanceof InlineCustomMapper);
    Principal principal = mapper.map("foo");
    assertEquals("inline:foo", principal.getName());
  }
}
