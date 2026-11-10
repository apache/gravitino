/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import java.time.Instant;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergEntityAuditUtils {

  @Test
  public void testCreatorIsSetOnlyWhenMissing() {
    Instant now = Instant.now();
    AuditInfo empty = IcebergEntityAuditUtils.withCreator(AuditInfo.EMPTY, "alice", now);
    Assertions.assertEquals("alice", empty.creator());
    Assertions.assertEquals(now, empty.createTime());
    Assertions.assertNull(empty.lastModifier());

    AuditInfo modifiedOnly =
        AuditInfo.builder().withLastModifier("bob").withLastModifiedTime(now).build();
    AuditInfo filled = IcebergEntityAuditUtils.withCreator(modifiedOnly, "alice", now);
    Assertions.assertEquals("alice", filled.creator());
    Assertions.assertEquals("bob", filled.lastModifier());

    AuditInfo reported =
        AuditInfo.builder().withCreator("carol").withCreateTime(Instant.EPOCH).build();
    Assertions.assertSame(
        reported, IcebergEntityAuditUtils.withCreator(reported, "alice", Instant.now()));
  }
}
