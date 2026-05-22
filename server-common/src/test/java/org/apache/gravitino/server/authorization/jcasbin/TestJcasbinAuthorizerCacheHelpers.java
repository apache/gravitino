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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for the cached role relation snapshot classes used by the version-validated caches. */
public class TestJcasbinAuthorizerCacheHelpers {

  @Test
  void testCachedUserRoleRels() {
    List<Long> roleIds = Arrays.asList(10L, 20L, 30L);
    CachedUserRoleRels cur = new CachedUserRoleRels(1L, 1000L, roleIds);
    Assertions.assertEquals(1L, cur.getUserId());
    Assertions.assertEquals(1000L, cur.getUpdatedAt());
    Assertions.assertEquals(roleIds, cur.getRoleIds());
  }

  @Test
  void testCachedGroupRoleRels() {
    List<Long> roleIds = Arrays.asList(100L, 200L);
    CachedGroupRoleRels cgr = new CachedGroupRoleRels(5L, 2000L, roleIds);
    Assertions.assertEquals(5L, cgr.getGroupId());
    Assertions.assertEquals(2000L, cgr.getUpdatedAt());
    Assertions.assertEquals(roleIds, cgr.getRoleIds());
  }
}
