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

import java.io.IOException;
import java.util.ServiceLoader;
import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.relational.LegacyDataCleaner;
import org.apache.gravitino.storage.relational.LegacyDataCleanerLoader;

/** Core facade for built-in IdP group legacy metadata cleanup. */
public class IdpGroupMetaService {
  private static final IdpGroupMetaService INSTANCE = new IdpGroupMetaService();

  public static IdpGroupMetaService getInstance() {
    return INSTANCE;
  }

  private IdpGroupMetaService() {}

  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) throws IOException {
    return deleteGroupMetasByLegacyTimelineWithCleaner(
        legacyTimeline, limit, ServiceLoader.load(LegacyDataCleaner.class));
  }

  int deleteGroupMetasByLegacyTimelineWithCleaner(
      long legacyTimeline, int limit, Iterable<LegacyDataCleaner> cleaners) throws IOException {
    return LegacyDataCleanerLoader.load(Entity.EntityType.IDP_GROUP, cleaners)
        .hardDeleteLegacyData(legacyTimeline, limit);
  }
}
