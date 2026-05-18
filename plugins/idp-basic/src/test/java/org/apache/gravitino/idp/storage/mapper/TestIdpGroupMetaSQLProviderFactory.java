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

package org.apache.gravitino.idp.storage.mapper;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.junit.jupiter.api.Test;

public class TestIdpGroupMetaSQLProviderFactory {

  @Test
  public void testGetProviderForSupportedBackends() {
    assertSame(IdpGroupMetaSQLProviderFactory.mysqlProvider(), getProvider("mysql"));
    assertSame(IdpGroupMetaSQLProviderFactory.h2Provider(), getProvider("h2"));
    assertSame(IdpGroupMetaSQLProviderFactory.postgresqlProvider(), getProvider("postgresql"));
  }

  @Test
  public void testGetProviderWithNullDatabaseId() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> getProvider(null));

    assertTrue(exception.getMessage().contains("MyBatis databaseId is not configured"));
  }

  @Test
  public void testGetProviderWithUnsupportedDatabaseId() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> getProvider("sqlite"));

    assertTrue(exception.getMessage().contains("Unsupported IdP group SQL provider databaseId"));
  }

  private IdpGroupMetaBaseSQLProvider getProvider(String databaseId) {
    return IdpGroupMetaSQLProviderFactory.getProvider(databaseId);
  }
}
