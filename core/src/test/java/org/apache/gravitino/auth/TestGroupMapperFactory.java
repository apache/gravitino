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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TestGroupMapperFactory {

  @Test
  public void testCreateRegexMapper() {
    GroupMapper mapper = GroupMapperFactory.create("regex", "group-(.*)");

    assertNotNull(mapper);
    assertTrue(mapper instanceof RegexGroupMapper);

    List<String> groups = Arrays.asList("group-admin", "group-user");
    List<String> mappedGroups = mapper.map(groups);

    assertEquals(2, mappedGroups.size());
    assertTrue(mappedGroups.contains("admin"));
    assertTrue(mappedGroups.contains("user"));
  }

  @Test
  public void testCreateRegexMapperWithDefaultPattern() {
    GroupMapper mapper = GroupMapperFactory.create("regex", "^(.*)$");

    assertNotNull(mapper);
    assertTrue(mapper instanceof RegexGroupMapper);

    List<String> groups = Arrays.asList("admin", "user");
    List<String> mappedGroups = mapper.map(groups);

    assertEquals(2, mappedGroups.size());
    assertTrue(mappedGroups.contains("admin"));
    assertTrue(mappedGroups.contains("user"));
  }

  @Test
  public void testCreateRegexMapperWithSlash() {
    GroupMapper mapper = GroupMapperFactory.create("regex", "/(.*)");

    assertNotNull(mapper);
    assertTrue(mapper instanceof RegexGroupMapper);

    List<String> groups = Arrays.asList("/admin", "/user");
    List<String> mappedGroups = mapper.map(groups);

    assertEquals(2, mappedGroups.size());
    assertTrue(mappedGroups.contains("admin"));
    assertTrue(mappedGroups.contains("user"));
  }

  @Test
  public void testCreateCustomMapperWithInvalidClass() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              GroupMapperFactory.create("unknown.InvalidClass", null);
            });
    assertTrue(exception.getMessage().contains("Failed to create GroupMapper"));
  }

  public static class TestCustomGroupMapper implements GroupMapper {
    @Override
    public List<String> map(List<String> groups) {
      if (groups == null) {
        return Collections.emptyList();
      }
      return groups.stream().map(g -> "custom:" + g).collect(Collectors.toList());
    }
  }

  @Test
  public void testCreateCustomMapperWithInlineClass() {
    String className = TestCustomGroupMapper.class.getName();
    GroupMapper mapper = GroupMapperFactory.create(className, null);

    assertNotNull(mapper);
    assertTrue(mapper instanceof TestCustomGroupMapper);

    List<String> groups = Collections.singletonList("foo");
    List<String> mappedGroups = mapper.map(groups);

    assertEquals(1, mappedGroups.size());
    assertEquals("custom:foo", mappedGroups.get(0));
  }
}
