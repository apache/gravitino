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
package org.apache.gravitino.idp.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.idp.authorization.IdpAuthorizationUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Test;

public class TestIdpUserEntity {

  @Test
  void testBuilderAndType() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("admin").withCreateTime(Instant.now()).build();
    IdpUserEntity entity =
        IdpUserEntity.builder()
            .withId(1L)
            .withName("alice")
            .withNamespace(IdpAuthorizationUtils.ofIdpUserNamespace())
            .withGroupNames(Lists.newArrayList("dev"))
            .withAuditInfo(auditInfo)
            .withPasswordHash("hash")
            .build();

    assertEquals(IdpEntityType.IDP_USER, entity.type());
    assertEquals("alice", entity.name());
    assertEquals(1L, entity.id());
    assertEquals(Lists.newArrayList("dev"), entity.groupNames());
    assertEquals(auditInfo, entity.auditInfo());
  }
}
