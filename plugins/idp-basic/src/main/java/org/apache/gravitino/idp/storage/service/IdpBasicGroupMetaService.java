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
package org.apache.gravitino.idp.storage.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Built-in IdP group metadata garbage collection service. */
public class IdpBasicGroupMetaService {

  /** Creates a new service instance. */
  public IdpBasicGroupMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteIdpGroupMetasByLegacyTimeline")
  public int deleteGroupMetasByLegacyTimeline(long legacyTimeline, int limit) {
    AtomicInteger groupDeletedCount = new AtomicInteger();
    AtomicInteger relDeletedCount = new AtomicInteger();

    SessionUtils.doMultipleWithCommit(
        () -> groupDeletedCount.set(deleteIdpGroupMetas(legacyTimeline, limit)),
        () -> relDeletedCount.set(deleteIdpUserGroupRelMetas(legacyTimeline, limit)));

    return groupDeletedCount.get() + relDeletedCount.get();
  }

  private int deleteIdpGroupMetas(long legacyTimeline, int limit) {
    Integer deletedCount =
        SessionUtils.getWithoutCommit(
            IdpGroupMetaMapper.class,
            mapper -> mapper.deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit));
    return deletedCount == null ? 0 : deletedCount;
  }

  private int deleteIdpUserGroupRelMetas(long legacyTimeline, int limit) {
    Integer deletedCount =
        SessionUtils.getWithoutCommit(
            IdpUserGroupRelMapper.class,
            mapper -> mapper.deleteIdpUserGroupRelMetasByLegacyTimeline(legacyTimeline, limit));
    return deletedCount == null ? 0 : deletedCount;
  }
}
