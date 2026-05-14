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

package org.apache.gravitino.idp.basic.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaSQLProviderFactoryFailure {

  @Test
  void testGetProviderThrowsForUnsupportedDatabaseId() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> IdpUserMetaSQLProviderFactory.getProvider("sqlite"));

    assertTrue(exception.getMessage().contains("sqlite"));
    assertTrue(exception.getMessage().contains("supported backends"));
  }

  @Test
  void testGetProviderThrowsForMissingDatabaseId() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> IdpUserMetaSQLProviderFactory.getProvider(null));

    assertTrue(exception.getMessage().contains("databaseId"));
    assertTrue(exception.getMessage().contains("not configured"));
  }

  @Test
  void testGetProviderThrowsWhenResolvedBackendHasNoProvider() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                IdpUserMetaSQLProviderFactory.getProvider(
                    JDBCBackendType.H2, "h2", ImmutableMap.of()));

    assertTrue(exception.getMessage().contains("No IdP user SQL provider registered"));
    assertTrue(exception.getMessage().contains("h2"));
  }
}
