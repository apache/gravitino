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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.relational.LegacyDataCleaner;
import org.junit.jupiter.api.Test;

public class TestJDBCBackendLegacyCleanup {

  @Test
  public void testDeleteLegacyDataWithPluginCleaner() throws IOException {
    assertEquals(
        3,
        IdpUserMetaService.getInstance()
            .deleteUserMetasByLegacyTimelineWithCleaner(
                1L, 10, List.of(new FakeCleaner(Entity.EntityType.IDP_USER, 3))));
    assertEquals(
        5,
        IdpGroupMetaService.getInstance()
            .deleteGroupMetasByLegacyTimelineWithCleaner(
                2L, 10, List.of(new FakeCleaner(Entity.EntityType.IDP_GROUP, 5))));
  }

  @Test
  public void testDeleteLegacyDataWithPluginCleanerFailsWhenCleanerMissing() {
    assertThrows(
        IOException.class,
        () ->
            IdpUserMetaService.getInstance()
                .deleteUserMetasByLegacyTimelineWithCleaner(
                    1L, 10, List.of(new FakeCleaner(Entity.EntityType.IDP_GROUP, 5))));
  }

  @Test
  public void testDeleteLegacyDataWithPluginCleanerFailsWhenMultipleCleanersFound() {
    assertThrows(
        IOException.class,
        () ->
            IdpUserMetaService.getInstance()
                .deleteUserMetasByLegacyTimelineWithCleaner(
                    1L,
                    10,
                    List.of(
                        new FakeCleaner(Entity.EntityType.IDP_USER, 3),
                        new FakeCleaner(Entity.EntityType.IDP_USER, 4))));
  }

  private static class FakeCleaner implements LegacyDataCleaner {
    private final Entity.EntityType entityType;
    private final int deletedCount;

    private FakeCleaner(Entity.EntityType entityType, int deletedCount) {
      this.entityType = entityType;
      this.deletedCount = deletedCount;
    }

    @Override
    public Entity.EntityType entityType() {
      return entityType;
    }

    @Override
    public int hardDeleteLegacyData(long legacyTimeline, int limit) {
      return deletedCount;
    }
  }
}
