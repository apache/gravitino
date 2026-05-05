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
package org.apache.gravitino.auth.local.storage.relational.mapper.provider.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpAuthenticationBaseSQLProvider {

  @Test
  void testSoftDeleteLocalUserDoesNotUseOptimisticLocking() {
    IdpAuthenticationBaseSQLProvider provider = new IdpAuthenticationBaseSQLProvider();

    String normalizedSql =
        provider.softDeleteLocalUser(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.contains("current_version = #{currentVersion}"),
        "Built-in IdP user delete should not use optimistic locking");
    Assertions.assertTrue(
        normalizedSql.contains("current_version = current_version + 1"),
        "Built-in IdP user delete should increment current_version in place");
    Assertions.assertTrue(
        normalizedSql.contains("last_version = last_version + 1"),
        "Built-in IdP user delete should increment last_version in place");
  }

  @Test
  void testSoftDeleteLocalGroupDoesNotUseOptimisticLocking() {
    IdpAuthenticationBaseSQLProvider provider = new IdpAuthenticationBaseSQLProvider();

    String normalizedSql =
        provider.softDeleteLocalGroup(1L, 2L, "audit").replaceAll("\\s+", " ").trim();

    Assertions.assertFalse(
        normalizedSql.contains("current_version = #{currentVersion}"),
        "Built-in IdP group delete should not use optimistic locking");
    Assertions.assertTrue(
        normalizedSql.contains("current_version = current_version + 1"),
        "Built-in IdP group delete should increment current_version in place");
    Assertions.assertTrue(
        normalizedSql.contains("last_version = last_version + 1"),
        "Built-in IdP group delete should increment last_version in place");
  }
}
