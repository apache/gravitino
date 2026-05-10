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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class TestJDBCBackendLegacyCleanup {

  @Test
  public void testDeleteLegacyDataWithPluginService() throws IOException {
    JDBCBackend backend = new JDBCBackend();

    assertEquals(
        3,
        backend.deleteLegacyDataWithPluginService(
            FakeIdpUserMetaService.class.getName(), "deleteUserMetasByLegacyTimeline", 1L));
    assertEquals(
        5,
        backend.deleteLegacyDataWithPluginService(
            FakeIdpGroupMetaService.class.getName(), "deleteGroupMetasByLegacyTimeline", 2L));
  }

  @Test
  public void testDeleteLegacyDataWithPluginServiceFailsWhenServiceMissing() {
    JDBCBackend backend = new JDBCBackend();

    assertThrows(
        IOException.class,
        () ->
            backend.deleteLegacyDataWithPluginService(
                "org.apache.gravitino.storage.relational.service.MissingService",
                "deleteUserMetasByLegacyTimeline",
                1L));
  }

  public static class FakeIdpUserMetaService {
    public static FakeIdpUserMetaService getInstance() {
      return new FakeIdpUserMetaService();
    }

    public int deleteUserMetasByLegacyTimeline(long legacyTimeline, int limit) {
      return 3;
    }
  }

  public static class FakeIdpGroupMetaService {
    public static FakeIdpGroupMetaService getInstance() {
      return new FakeIdpGroupMetaService();
    }

    public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
      return 5;
    }
  }
}
