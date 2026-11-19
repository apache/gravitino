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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/**
 * Stores glossary edges and serializes hierarchy changes per metalake using a database mutex.
 *
 * <p>Writes join an enclosing SessionUtils transaction or commit their own. Lock order is the live
 * metalake fence, hierarchy mutex, then live term rows in ascending ID order. A missing metalake or
 * endpoint throws NoSuchEntityException. Duplicate additions and absent removals return false.
 * RELATED pairs are stored with the lower ID first. Self-links are rejected with
 * IllegalArgumentException; cycles, depth, ancestor/RELATED overlap, authorization, revisions and
 * audit changes belong to the manager (T4/T5), not this storage layer.
 *
 * <p>Use withHierarchyLock around the complete read/validate/write operation, before locking any
 * terms. Multiple changes must plan and lock all their term IDs in order before making changes.
 * Plain reads take no locks and return only live endpoints. Each query has a READ_COMMITTED
 * statement snapshot; a multi-level walk needs an enclosing hierarchy lock for a stable graph.
 * These ID-only reads do not apply authorization/status filters: the manager must filter each
 * frontier before expanding it.
 */
public final class GlossaryTermRelService {
  private static final int FRONTIER_BATCH_SIZE = 500;
  private static final GlossaryTermRelService INSTANCE = new GlossaryTermRelService();

  /** The two persisted edge kinds. BROADER points from child to parent. */
  public enum RelationType {
    /** A directed child-to-parent edge. */
    BROADER,
    /** An undirected pair stored in ascending ID order. */
    RELATED
  }

  private GlossaryTermRelService() {}

  /** Returns the shared relation storage service. */
  public static GlossaryTermRelService getInstance() {
    return INSTANCE;
  }

  /**
   * Runs work under the live metalake fence and exclusive database hierarchy mutex. The lock stays
   * held until the outermost transaction commits or rolls back, including after this method
   * returns. Cached reads made before acquiring the lock are discarded. A standalone failure rolls
   * back this operation. Nested transactions have no savepoints: an enclosing owner must propagate
   * the failure or explicitly roll back, rather than catch it and commit partial work. The callback
   * must not acquire new locks earlier than the hierarchy mutex in lock order. Reacquiring an
   * already-held parent fence in the same mode (as add/remove do) is safe.
   *
   * @param metalakeId the live metalake ID
   * @param work the complete read/validate/write operation
   * @param <T> the result type
   * @return the callback result
   */
  public <T> T withHierarchyLock(long metalakeId, Supplier<T> work) {
    Preconditions.checkArgument(work != null, "work cannot be null");
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermRelMapper.class,
        mapper -> {
          SessionUtils.clearSessionCache();
          if (SessionUtils.getWithoutCommit(
                  MetalakeMetaMapper.class, m -> m.selectMetalakeMetaByIdForShare(metalakeId))
              == null) {
            throw new NoSuchEntityException("Metalake %s does not exist", metalakeId);
          }
          lockHierarchy(metalakeId);
          return work.get();
        });
  }

  /**
   * Runs work after acquiring the metalake fence, hierarchy mutex and all planned live endpoints in
   * ascending term ID order. A missing endpoint prevents the callback from running.
   */
  public <T> T withHierarchyLock(long metalakeId, Set<Long> termIds, Supplier<T> work) {
    checkIds(termIds);
    Preconditions.checkArgument(work != null, "work cannot be null");
    return withHierarchyLock(
        metalakeId,
        () -> {
          SessionUtils.doWithoutCommit(
              GlossaryTermRelMapper.class,
              mapper -> {
                for (Long id : termIds.stream().sorted().toList()) {
                  if (mapper.lockTerm(metalakeId, id) == null) {
                    throw new NoSuchEntityException(
                        "Glossary term %s does not exist in metalake %s", id, metalakeId);
                  }
                }
              });
          return work.get();
        });
  }

  /**
   * Adds an edge if absent, verifying both live endpoints in this metalake. Graph semantic checks
   * and revision changes are the caller's responsibility under withHierarchyLock.
   */
  public boolean add(long metalakeId, long fromId, long toId, RelationType type) {
    checkEdge(fromId, toId, type);
    long from = type == RelationType.RELATED ? Math.min(fromId, toId) : fromId;
    long to = type == RelationType.RELATED ? Math.max(fromId, toId) : toId;
    return withHierarchyLock(
        metalakeId,
        Set.of(from, to),
        () ->
            SessionUtils.getWithoutCommit(
                GlossaryTermRelMapper.class,
                mapper -> {
                  if (mapper.exists(metalakeId, from, to, type.name()) != 0) {
                    return false;
                  }
                  mapper.insert(metalakeId, from, to, type.name());
                  return true;
                }));
  }

  /** Removes a live edge if present. Both endpoints must still exist in this metalake. */
  public boolean remove(long metalakeId, long fromId, long toId, RelationType type) {
    checkEdge(fromId, toId, type);
    long from = type == RelationType.RELATED ? Math.min(fromId, toId) : fromId;
    long to = type == RelationType.RELATED ? Math.max(fromId, toId) : toId;
    return withHierarchyLock(
        metalakeId,
        Set.of(from, to),
        () ->
            SessionUtils.getWithoutCommit(
                GlossaryTermRelMapper.class,
                mapper ->
                    mapper.remove(
                            metalakeId, from, to, type.name(), nextDeletedAt(mapper, metalakeId))
                        == 1));
  }

  /** Returns the live direct broader IDs, or an empty set when none exist. */
  public Set<Long> listBroader(long metalakeId, long termId) {
    return neighbors(listBroaderEdges(metalakeId, Set.of(termId)), termId, false, false);
  }

  /** Returns the live direct narrower IDs, or an empty set when none exist. */
  public Set<Long> listNarrower(long metalakeId, long termId) {
    return neighbors(listNarrowerEdges(metalakeId, Set.of(termId)), termId, true, false);
  }

  /** Returns the live related IDs from either orientation of the stored pair. */
  public Set<Long> listRelated(long metalakeId, long termId) {
    return neighbors(
        readEdges(metalakeId, Set.of(termId), RelationType.RELATED, false), termId, false, true);
  }

  /**
   * Reads all outgoing BROADER edges of a frontier in bounded batches; an empty frontier runs no
   * SQL.
   */
  public List<GlossaryTermRelPO> listBroaderEdges(long metalakeId, Set<Long> termIds) {
    return readEdges(metalakeId, termIds, RelationType.BROADER, false);
  }

  /**
   * Reads all incoming BROADER edges of a frontier in bounded batches; an empty frontier runs no
   * SQL.
   */
  public List<GlossaryTermRelPO> listNarrowerEdges(long metalakeId, Set<Long> termIds) {
    return readEdges(metalakeId, termIds, RelationType.BROADER, true);
  }

  /** Reads each live RELATED pair touching the frontier once, in canonical endpoint order. */
  public List<GlossaryTermRelPO> listRelatedEdges(long metalakeId, Set<Long> ids) {
    return readEdges(metalakeId, ids, RelationType.RELATED, false);
  }

  /**
   * Walks all live ancestors in bounded batches at each level, including the terminal frontier. IDs
   * are visited once, even for diamonds or malformed cycles; the root is excluded.
   */
  public Set<Long> ancestors(long metalakeId, long termId) {
    return walk(metalakeId, termId, false);
  }

  /** Walks all live descendants with the same snapshot and deduplication rules as ancestors. */
  public Set<Long> descendants(long metalakeId, long termId) {
    return walk(metalakeId, termId, true);
  }

  /** Purges at most limit retired edges older than cutoff without touching live replacements. */
  public int collect(long cutoff, int limit) {
    Preconditions.checkArgument(limit >= 0, "limit must be nonnegative");
    if (limit == 0) {
      return 0;
    }
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermRelMapper.class,
        mapper -> {
          int count = 0;
          for (GlossaryTermRelPO row : mapper.expired(cutoff, limit)) {
            count += mapper.purge(row);
          }
          return count;
        });
  }

  // The caller already holds the live parent fence in the current transaction.
  void lockHierarchy(long metalakeId) {
    Preconditions.checkState(
        SessionUtils.isInTransaction(), "Hierarchy locks require a transaction");
    SessionUtils.doWithoutCommit(
        GlossaryTermRelMapper.class,
        mapper -> {
          mapper.ensureLock(metalakeId);
          Preconditions.checkState(
              mapper.lockHierarchy(metalakeId) != null,
              "Missing glossary hierarchy mutex for %s",
              metalakeId);
        });
    SessionUtils.clearSessionCache();
  }

  // Storage cleanup only. The manager plans surviving-term revision/audit changes in T5.
  void removeTerm(long metalakeId, long termId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermRelMapper.class,
        mapper -> mapper.removeTerm(metalakeId, termId, nextDeletedAt(mapper, metalakeId)));
  }

  // Exclusive metalake deletion excludes all hierarchy-lock holders and waiters via the parent
  // fence.
  void dropMetalake(long metalakeId) {
    SessionUtils.doWithoutCommit(
        GlossaryTermRelMapper.class,
        mapper -> {
          mapper.dropMetalake(metalakeId, nextDeletedAt(mapper, metalakeId));
          mapper.dropLock(metalakeId);
        });
  }

  private List<GlossaryTermRelPO> readEdges(
      long metalakeId, Set<Long> ids, RelationType type, boolean reverse) {
    checkIds(ids);
    if (ids.isEmpty()) {
      return Collections.emptyList();
    }
    return SessionUtils.getWithoutCommit(
        GlossaryTermRelMapper.class,
        mapper -> {
          if (ids.size() <= FRONTIER_BATCH_SIZE) {
            return mapper.listEdges(metalakeId, ids, type.name(), reverse);
          }
          // A symmetric pair can match one endpoint in each of two batches.
          Set<GlossaryTermRelPO> edges =
              new TreeSet<>(
                  Comparator.comparing(GlossaryTermRelPO::getFromTermId)
                      .thenComparing(GlossaryTermRelPO::getToTermId));
          for (List<Long> batch : Lists.partition(List.copyOf(ids), FRONTIER_BATCH_SIZE)) {
            edges.addAll(mapper.listEdges(metalakeId, batch, type.name(), reverse));
          }
          return new ArrayList<>(edges);
        });
  }

  private Set<Long> walk(long metalakeId, long root, boolean reverse) {
    Set<Long> visited = new LinkedHashSet<>();
    visited.add(root);
    Set<Long> frontier = Set.of(root);
    while (!frontier.isEmpty()) {
      List<GlossaryTermRelPO> edges =
          reverse
              ? listNarrowerEdges(metalakeId, frontier)
              : listBroaderEdges(metalakeId, frontier);
      Set<Long> next = new LinkedHashSet<>();
      for (GlossaryTermRelPO edge : edges) {
        long id = reverse ? edge.getFromTermId() : edge.getToTermId();
        if (visited.add(id)) {
          next.add(id);
        }
      }
      frontier = next;
    }
    visited.remove(root);
    return visited;
  }

  private Set<Long> neighbors(
      List<GlossaryTermRelPO> edges, long root, boolean reverse, boolean related) {
    Set<Long> result = new LinkedHashSet<>();
    for (GlossaryTermRelPO edge : edges) {
      result.add(
          reverse || (related && edge.getToTermId() == root)
              ? edge.getFromTermId()
              : edge.getToTermId());
    }
    return result;
  }

  private void checkEdge(long fromId, long toId, RelationType type) {
    Preconditions.checkArgument(type != null, "relation type cannot be null");
    Preconditions.checkArgument(fromId != toId, "A glossary term cannot be linked to itself");
  }

  private void checkIds(Set<Long> ids) {
    Preconditions.checkArgument(ids != null, "term IDs cannot be null");
    for (Long id : ids) {
      Preconditions.checkArgument(id != null, "term ID cannot be null");
    }
  }

  private long nextDeletedAt(GlossaryTermRelMapper mapper, long metalakeId) {
    // The mutex serializes tombstone generation; PostgreSQL's clock or repeated same-ms changes
    // must not collide with retained generations of the same edge. The index starts with
    // metalake_id.
    return Math.max(
        Math.max(1L, System.currentTimeMillis()),
        Math.addExact(mapper.lastDeletedAt(metalakeId), 1L));
  }
}
