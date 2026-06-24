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

package org.apache.gravitino.idp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestIdpUserGroupsCache {

  private static final String USERNAME = "alice";

  private Config config;
  private IdpUserGroupsCache userGroupsCache;

  @BeforeEach
  void setUp() {
    config = new Config(false) {};
    config.set(Configs.BASIC_AUTHENTICATION_GROUPS_CACHE_TTL_SECS, 60L);
    config.set(Configs.BASIC_AUTHENTICATION_GROUPS_CACHE_SIZE, 100L);
    userGroupsCache = new IdpUserGroupsCache(config);
  }

  @Test
  void testCacheHit() {
    AtomicInteger loadCount = new AtomicInteger();
    List<String> groupNames = Arrays.asList("group-a", "group-b");

    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));
    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));

    assertEquals(1, loadCount.get());
  }

  @Test
  void testInvalidateUser() {
    AtomicInteger loadCount = new AtomicInteger();
    List<String> groupNames = Arrays.asList("group-a");

    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));
    userGroupsCache.invalidate(USERNAME);
    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));

    assertEquals(2, loadCount.get());
  }

  @Test
  void testInvalidateUsers() {
    AtomicInteger loadCount = new AtomicInteger();
    List<String> groupNames = Arrays.asList("group-a");

    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));
    userGroupsCache.invalidate(Collections.singletonList(USERNAME));
    userGroupsCache.get(USERNAME, () -> load(loadCount, groupNames));

    assertEquals(2, loadCount.get());
  }

  private static List<String> load(AtomicInteger loadCount, List<String> groupNames) {
    loadCount.incrementAndGet();
    return groupNames;
  }
}
