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

package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.gravitino.storage.relational.mapper.provider.h2.IdpUserMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.mysql.IdpUserMetaMySQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.IdpUserMetaPostgreSQLProvider;
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaSQLProviderFactory {

  @Test
  void testGetProviderReturnsMySQLProvider() {
    assertInstanceOf(
        IdpUserMetaMySQLProvider.class, IdpUserMetaSQLProviderFactory.getProvider("mysql"));
  }

  @Test
  void testGetProviderReturnsH2Provider() {
    assertInstanceOf(IdpUserMetaH2Provider.class, IdpUserMetaSQLProviderFactory.getProvider("h2"));
  }

  @Test
  void testGetProviderReturnsPostgreSQLProvider() {
    assertInstanceOf(
        IdpUserMetaPostgreSQLProvider.class,
        IdpUserMetaSQLProviderFactory.getProvider("postgresql"));
  }
}
