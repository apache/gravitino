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
package org.apache.gravitino.idp.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Configs;
import org.apache.gravitino.idp.authorization.IdpAuthorizationUtils;
import org.apache.gravitino.idp.meta.IdpEntityType;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.storage.mapper.AbstractIdpMetaStorageTest;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIdpJDBCBackend extends AbstractIdpMetaStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testIdpUserCrud(String backendType) throws IOException {
    init(backendType);

    getConfig().set(Configs.CACHE_ENABLED, false);
    IdpJDBCBackend idpBackend = new IdpJDBCBackend();
    idpBackend.initialize(getConfig());

    IdpUserEntity user =
        IdpUserEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("bob")
            .withNamespace(IdpAuthorizationUtils.ofIdpUserNamespace())
            .withPasswordHash("hash")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    idpBackend.insert(user, false);

    assertTrue(idpBackend.exists(IdpAuthorizationUtils.ofIdpUser("bob"), IdpEntityType.IDP_USER));
    assertEquals(
        "bob",
        idpBackend.get(IdpAuthorizationUtils.ofIdpUser("bob"), IdpEntityType.IDP_USER).name());
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            idpBackend.list(
                IdpAuthorizationUtils.ofIdpUserNamespace(), IdpEntityType.IDP_USER, true));
    assertTrue(
        idpBackend.delete(IdpAuthorizationUtils.ofIdpUser("bob"), IdpEntityType.IDP_USER, false));
    assertFalse(idpBackend.exists(IdpAuthorizationUtils.ofIdpUser("bob"), IdpEntityType.IDP_USER));

    idpBackend.close();
  }
}
