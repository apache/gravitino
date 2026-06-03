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
import org.apache.gravitino.storage.relational.EntityChangeLogListener;
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
 * <p>This class polls {@code owner_meta} itself and receives {@code entity_change_log} batches from
 * the global entity change log poller.
 *
 * <p>Other Gravitino nodes therefore observe ALTER/DROP and owner changes within one poll interval.
 */
public class JcasbinChangeListener implements EntityChangeLogListener, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinChangeListener.class);

  private final GravitinoCache<String, Long> metadataIdCache;
  private final GravitinoCache<Long, Optional<OwnerInfo>> ownerRelCache;
  private final long pollIntervalSecs;

  private ScheduledExecutorService scheduler;

  // (ownerPollHighWaterUpdatedAt, ownerPollHighWaterUpdatedAtId) is a single logical keyset cursor
  // over owner_meta. Inserts set updated_at = now (in POConverters) and soft-deletes set it via
  // SQL, so every write advances updated_at — one cursor catches both. id is the tiebreaker for
  // batch soft-deletes (softDeleteOwnerRelByCatalogId etc.) where many rows share the same ms.
  private volatile long ownerPollHighWaterUpdatedAt = 0;
  private volatile long ownerPollHighWaterUpdatedAtId = 0;

  /**
   * @param metadataIdCache the metadata-id cache to invalidate on entity changes
   * @param ownerRelCache the owner cache to invalidate on owner changes
   * @param pollIntervalSecs interval between successive polling cycles
   */
  public JcasbinChangeListener(
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
   * <p>The owner poller advances a single {@code (updated_at, id)} keyset cursor. {@code id} alone
   * would miss soft-delete updates (which reuse the row id), and {@code updated_at} alone would
   * miss same-millisecond rows from batch soft-deletes (which all share one {@code updated_at}).
   */
  public void start() {
    ChangedOwnerInfo maxOwnerChange =
        SessionUtils.getWithoutCommit(
            OwnerMetaMapper.class, OwnerMetaMapper::selectMaxChangedOwner);
    if (maxOwnerChange != null) {
      ownerPollHighWaterUpdatedAt = maxOwnerChange.getUpdatedAt();
      ownerPollHighWaterUpdatedAtId = maxOwnerChange.getId();
    }

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
          "Polling for owner changes after (updated_at={}, id={})",
          ownerPollHighWaterUpdatedAt,
          ownerPollHighWaterUpdatedAtId);
      pollOwnerChanges();
    } catch (Exception e) {
      if (handleInterruptIfAny(e, "Owner change poll")) {
        return;
      }
      LOG.warn("Owner change poll failed", e);
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
   * #ownerPollHighWaterUpdatedAtId} and invalidates the affected {@code ownerRelCache} entries.
   * Each row carries {@code metadataObjectId}, so invalidation is a direct key removal — no name
   * resolution needed.
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
            m -> m.selectChangedOwners(ownerPollHighWaterUpdatedAt, ownerPollHighWaterUpdatedAtId));
    if (changes.isEmpty()) {
      return;
    }

    long[] maxSeenUpdatedAt = {ownerPollHighWaterUpdatedAt};
    long[] maxSeenUpdatedAtId = {ownerPollHighWaterUpdatedAtId};
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
          }
        });
    ownerPollHighWaterUpdatedAt = maxSeenUpdatedAt[0];
    ownerPollHighWaterUpdatedAtId = maxSeenUpdatedAtId[0];
  }

  /**
   * Invalidates the affected {@code metadataIdCache} keys from an entity-change batch.
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
  @Override
  public synchronized void onEntityChange(List<EntityChangeRecord> changes) {
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
        continue;
      }

      MetadataObject mdObj = metadataObjectFromChangeLog(metalake, fullName, mdType);
      String cacheKey = JcasbinAuthorizationCacheKeys.metadataIdCacheKey(metalake, mdObj);

      if (JcasbinAuthorizationCacheKeys.hasNestedMetadataObjects(mdType)) {
        addCoalescedPrefix(containerPrefixes, cacheKey);
      } else {
        leafKeys.add(cacheKey);
      }
    }
    invalidateCoalescedKeys(containerPrefixes, leafKeys);
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
