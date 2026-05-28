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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  /** Max entity-change rows to fetch per poller cycle. */
  private static final int ENTITY_CHANGE_POLLER_MAX_ROWS = 500;

  private final GravitinoCache<String, Long> metadataIdCache;
  private final GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;
  private final long pollIntervalSecs;

  private ScheduledExecutorService scheduler;

  // The owner poller needs two cursors because owner_meta rows can be both inserted and
  // soft-deleted (which keeps the same id but advances updated_at):
  //   * (ownerPollHighWaterUpdatedAt, ownerPollHighWaterUpdatedAtId) is a single logical cursor
  //     tracking soft-delete updates. updated_at alone is millisecond-granular so multiple
  //     updates can share the same value; the id field is the tiebreaker for that case.
  //   * ownerPollHighWaterInsertId tracks brand-new owner_meta rows by their auto-increment id.
  private volatile long ownerPollHighWaterUpdatedAt = 0;
  private volatile long ownerPollHighWaterUpdatedAtId = 0;
  private volatile long ownerPollHighWaterInsertId = 0;
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
    Preconditions.checkArgument(pollIntervalSecs > 0, "pollIntervalSecs must be positive");
    this.metadataIdCache = metadataIdCache;
    this.ownerRelCache = ownerRelCache;
    this.pollIntervalSecs = pollIntervalSecs;
  }

  /**
   * Initializes the high-water cursors to the current DB tail (so startup does not scan historical
   * changes) and schedules periodic polling.
   *
   * <p>The owner poller tracks inserts by id and updates by {@code (updated_at, id)} because owner
   * deletion paths soft-delete existing {@code owner_meta} rows. Those updates keep the same id but
   * advance {@code updated_at}; an id-only cursor would miss them on peer nodes.
   */
  public void start() {
    ChangedOwnerInfo maxOwnerChange =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangedOwner);
    if (maxOwnerChange != null) {
      ownerPollHighWaterUpdatedAt = maxOwnerChange.getUpdatedAt();
      ownerPollHighWaterUpdatedAtId = maxOwnerChange.getId();
    }
    ownerPollHighWaterInsertId =
        getOrDefault(
            SessionUtils.getWithoutCommit(
                OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangeId));
    entityPollHighWaterId =
        getOrDefault(
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
      LOG.debug(
          "Polling for owner changes after updated_at {}, updated_at_id {}, insert_id {}",
          ownerPollHighWaterUpdatedAt,
          ownerPollHighWaterUpdatedAtId,
          ownerPollHighWaterInsertId);
      pollOwnerChanges();
    } catch (Exception e) {
      if (handleInterruptIfAny(e, "Owner change poll")) {
        return;
      }
      LOG.warn("Owner change poll failed", e);
    }

    try {
      LOG.debug("Polling for entity changes after id {}", entityPollHighWaterId);
      pollEntityChanges();
    } catch (Exception e) {
      if (handleInterruptIfAny(e, "Entity change poll")) {
        return;
      }
      LOG.warn("Entity change poll failed", e);
    }
  }

  /**
   * Restores the interrupt flag and returns true if {@code e} carries (or the current thread has
   * accumulated) an interruption, so the caller can bail out of this poll cycle quickly during
   * {@link #close()}-driven {@code shutdownNow()}.
   */
  private static boolean handleInterruptIfAny(Throwable e, String context) {
    Throwable t = e;
    while (t != null) {
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        LOG.debug("{} interrupted, stopping poll cycle", context);
        return true;
      }
      t = t.getCause();
    }
    if (Thread.currentThread().isInterrupted()) {
      LOG.debug("{} ran while thread was interrupted, stopping poll cycle", context);
      return true;
    }
    return false;
  }

  /**
   * Drains owner-change rows past {@link #ownerPollHighWaterUpdatedAt}/{@link
   * #ownerPollHighWaterUpdatedAtId} and {@link #ownerPollHighWaterInsertId}, then invalidates the
   * affected {@code ownerRelCache} entries. Each row carries {@code metadataObjectId}, so
   * invalidation is a direct key removal — no name resolution needed.
   *
   * <p>The {@code synchronized} modifier is defensive. In production this method is only invoked
   * from the single-threaded scheduler started in {@link #start()}, and {@link
   * java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay} guarantees that
   * consecutive runs do not overlap. The cursor fields are also {@code volatile}, and cache
   * invalidations are now atomic at the cache layer via {@link
   * org.apache.gravitino.cache.GravitinoCache#runInvalidationBatch}. The keyword is kept so that
   * future callers — additional schedulers, ad-hoc invocations from tests or admin tooling — do not
   * silently introduce concurrent {@code "select changes → invalidate → advance cursor"} sequences.
   * Cost is negligible under no contention thanks to biased / elided locking.
   */
  private synchronized void pollOwnerChanges() {
    List<ChangedOwnerInfo> changes =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class,
            m ->
                m.selectChangedOwners(
                    ownerPollHighWaterUpdatedAt,
                    ownerPollHighWaterUpdatedAtId,
                    ownerPollHighWaterInsertId));
    if (changes.isEmpty()) {
      return;
    }

    long[] maxSeenUpdatedAt = {ownerPollHighWaterUpdatedAt};
    long[] maxSeenUpdatedAtId = {ownerPollHighWaterUpdatedAtId};
    long[] maxSeenInsertId = {ownerPollHighWaterInsertId};
    // Hold the cache's exclusive invalidation lock for the whole batch so readers never observe
    // a half-applied state where some of this batch's entries have been evicted and others are
    // still hot.
    ownerRelCache.runInvalidationBatch(
        () -> {
          for (ChangedOwnerInfo change : changes) {
            ownerRelCache.invalidate(change.getMetadataObjectId());
            if (change.getUpdatedAt() > maxSeenUpdatedAt[0]
                || (change.getUpdatedAt() == maxSeenUpdatedAt[0]
                    && change.getId() > maxSeenUpdatedAtId[0])) {
              maxSeenUpdatedAt[0] = change.getUpdatedAt();
              maxSeenUpdatedAtId[0] = change.getId();
            }
            if (change.getId() > maxSeenInsertId[0]) {
              maxSeenInsertId[0] = change.getId();
            }
          }
        });
    ownerPollHighWaterUpdatedAt = maxSeenUpdatedAt[0];
    ownerPollHighWaterUpdatedAtId = maxSeenUpdatedAtId[0];
    ownerPollHighWaterInsertId = maxSeenInsertId[0];
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
   *
   * <p>The {@code synchronized} modifier is defensive — see the note on {@link #pollOwnerChanges()}
   * for the rationale. The single-threaded scheduler already prevents overlapping runs in
   * production, and the per-batch invalidation atomicity is provided by the cache itself.
   */
  private synchronized void pollEntityChanges() {
    List<EntityChangeRecord> changes =
        SessionUtils.getWithoutCommit(
            EntityChangeLogMapper.class,
            m -> m.selectEntityChanges(entityPollHighWaterId, ENTITY_CHANGE_POLLER_MAX_ROWS));

    long maxSeenId = entityPollHighWaterId;
    Set<String> containerPrefixes = new LinkedHashSet<>();
    Set<String> leafKeys = new LinkedHashSet<>();
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
      String cacheKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, mdObj);

      if (JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(mdType)) {
        addCoalescedPrefix(containerPrefixes, cacheKey);
      } else {
        leafKeys.add(cacheKey);
      }

      if (change.getId() > maxSeenId) {
        maxSeenId = change.getId();
      }
    }
    invalidateCoalescedKeys(containerPrefixes, leafKeys);
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

  private static long getOrDefault(Long value) {
    return value == null ? 0L : value;
  }

  private static void addCoalescedPrefix(Set<String> prefixes, String candidate) {
    for (String prefix : prefixes) {
      if (candidate.startsWith(prefix)) {
        return;
      }
    }
    prefixes.removeIf(prefix -> prefix.startsWith(candidate));
    prefixes.add(candidate);
  }

  private void invalidateCoalescedKeys(Set<String> prefixes, Set<String> leafKeys) {
    if (prefixes.isEmpty() && leafKeys.isEmpty()) {
      return;
    }
    // Hold the cache's exclusive invalidation lock for the whole batch so readers never observe
    // a half-applied state where some prefix/leaf keys have been evicted and others have not.
    metadataIdCache.runInvalidationBatch(
        () -> {
          for (String prefix : prefixes) {
            metadataIdCache.invalidateByPrefix(prefix);
          }
          for (String leafKey : leafKeys) {
            if (prefixes.stream().noneMatch(leafKey::startsWith)) {
              metadataIdCache.invalidate(leafKey);
            }
          }
        });
  }
}
