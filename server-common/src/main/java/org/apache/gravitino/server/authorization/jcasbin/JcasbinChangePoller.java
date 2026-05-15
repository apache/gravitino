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
package org.apache.gravitino.server.authorization.jcasbin;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.cache.GravitinoCache;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.po.auth.ChangedOwnerInfo;
import org.apache.gravitino.storage.relational.po.auth.OwnerInfo;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Eventual-consistency invalidator for {@link JcasbinAuthorizer}'s {@code metadataIdCache} and
 * {@code ownerRelCache}.
 *
 * <p>One scheduled thread drains {@code entity_change_log} and {@code owner_meta} change rows since
 * a high-water-mark cursor and invalidates the affected keys. Other Gravitino nodes therefore
 * observe ALTER/DROP and owner changes within one poll interval.
 *
 * <p>Both polls run on every tick — a failure in one does not stop the other.
 */
public class JcasbinChangePoller implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinChangePoller.class);

  /** Max rows to fetch per poller cycle. */
  private static final int POLLER_MAX_ROWS = 500;

  private final GravitinoCache<String, Long> metadataIdCache;
  private final GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;
  private final long pollIntervalSecs;

  private ScheduledExecutorService scheduler;
  private volatile long ownerPollHighWaterId = 0;
  private volatile long entityPollHighWaterId = 0;

  /**
   * @param metadataIdCache the metadata-id cache to invalidate on entity changes
   * @param ownerRelCache the owner cache to invalidate on owner changes
   * @param pollIntervalSecs interval between successive polling cycles
   */
  public JcasbinChangePoller(
      GravitinoCache<String, Long> metadataIdCache,
      GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache,
      long pollIntervalSecs) {
    this.metadataIdCache = metadataIdCache;
    this.ownerRelCache = ownerRelCache;
    this.pollIntervalSecs = pollIntervalSecs;
  }

  /**
   * Initializes the high-water cursors to the current DB tail (so startup does not scan historical
   * changes) and schedules periodic polling.
   *
   * <p>Known trade-off: an id-based high-water mark can miss rows whose id is allocated before the
   * cursor snapshot but whose commit lands after it. Concretely, if writer A holds {@code id=N-1}
   * uncommitted while writer B commits {@code id=N}, {@code selectMaxChangeId()} returns N and the
   * next poll queries {@code id > N} — A's row is never consumed. In that case the affected cache
   * entry stays stale until either (a) a request-side path catches it on the next request, or (b)
   * TTL eviction. Acceptable for the eventual-consistency caches targeted here; revisit if we ever
   * route strong-consistency data through this poller.
   */
  public void start() {
    ownerPollHighWaterId =
        nullToZero(
            SessionUtils.getWithoutCommit(
                OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangeId));
    entityPollHighWaterId =
        nullToZero(
            SessionUtils.getWithoutCommit(
                EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId));

    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setName("GravitinoAuthorizer-ChangePoller");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleWithFixedDelay(
        this::pollChanges, pollIntervalSecs, pollIntervalSecs, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  void pollChanges() {
    try {
      LOG.debug("Polling for owner changes after id {}", ownerPollHighWaterId);
      pollOwnerChanges();
    } catch (Exception e) {
      LOG.warn("Owner change poll failed", e);
    }

    try {
      LOG.debug("Polling for entity changes after id {}", entityPollHighWaterId);
      pollEntityChanges();
    } catch (Exception e) {
      LOG.warn("Entity change poll failed", e);
    }
  }

  /**
   * Drains owner-change rows past {@link #ownerPollHighWaterId} and invalidates the affected {@code
   * ownerRelCache} entries. Each row carries {@code metadataObjectId}, so invalidation is a direct
   * key removal — no name resolution needed.
   */
  private void pollOwnerChanges() {
    List<ChangedOwnerInfo> changes =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, m -> m.selectChangedOwners(ownerPollHighWaterId));

    long maxSeenId = ownerPollHighWaterId;
    for (ChangedOwnerInfo change : changes) {
      ownerRelCache.invalidate(change.getMetadataObjectId());
      if (change.getId() > maxSeenId) {
        maxSeenId = change.getId();
      }
    }
    ownerPollHighWaterId = maxSeenId;
  }

  /**
   * Drains entity-change rows past {@link #entityPollHighWaterId} and invalidates the affected
   * {@code metadataIdCache} keys.
   *
   * <p><b>Contract with the writer side:</b> {@code entity_change_log.full_name} must be the
   * <i>pre-mutation</i> name (the name that consumers currently have cached). The writers in {@code
   * SchemaMetaService} / {@code TableMetaService} / etc. emit {@code oldFullName} on rename and the
   * current name on drop, so the cacheKey we build here resolves to the entry a peer node would
   * have populated under that name. If a future change starts emitting the new post-rename name,
   * this invalidation will silently miss and stale entries will only clear via LRU eviction.
   */
  private void pollEntityChanges() {
    List<EntityChangeRecord> changes =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class,
            m -> m.selectEntityChanges(entityPollHighWaterId, POLLER_MAX_ROWS));

    long maxSeenId = entityPollHighWaterId;
    for (EntityChangeRecord change : changes) {
      String metalake = change.getMetalakeName();
      String entityType = change.getEntityType();
      String fullName = change.getFullName();

      MetadataObject.Type mdType;
      try {
        mdType = MetadataObject.Type.valueOf(entityType.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown entity type in change log: {}", entityType);
        if (change.getId() > maxSeenId) {
          maxSeenId = change.getId();
        }
        continue;
      }

      MetadataObject mdObj = metadataObjectFromChangeLog(metalake, fullName, mdType);
      String cacheKey = JcasbinAuthorizationLookups.buildCacheKey(metalake, mdObj);

      if (JcasbinAuthorizationLookups.isNonLeaf(mdType)) {
        metadataIdCache.invalidateByPrefix(cacheKey);
      } else {
        metadataIdCache.invalidate(cacheKey);
      }

      if (change.getId() > maxSeenId) {
        maxSeenId = change.getId();
      }
    }
    entityPollHighWaterId = maxSeenId;
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  @VisibleForTesting
  static MetadataObject metadataObjectFromChangeLog(
      String metalake, String fullName, MetadataObject.Type type) {
    List<String> names = new ArrayList<>(Arrays.asList(fullName.split("\\.")));
    if (type != MetadataObject.Type.METALAKE
        && !names.isEmpty()
        && Objects.equals(names.get(0), metalake)) {
      names.remove(0);
    }
    return MetadataObjects.of(names, type);
  }

  private static long nullToZero(Long value) {
    return value == null ? 0L : value;
  }
}
