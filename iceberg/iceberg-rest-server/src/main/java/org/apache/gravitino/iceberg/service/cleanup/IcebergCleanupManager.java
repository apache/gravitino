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

package org.apache.gravitino.iceberg.service.cleanup;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-wide async cleanup engine: claims {@code iceberg_cleanup_job} rows, deletes the dropped
 * table's files in bulk, and renews claim heartbeats on a thread decoupled from deletion.
 */
public class IcebergCleanupManager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCleanupManager.class);

  private final IcebergCleanupJobStore store;
  private final int workerThreads;
  private final int deleteBatchSize;
  private final int maxAttempts;
  private final int candidateWindow;
  private final long pollIntervalMs;
  private final long heartbeatTimeoutMs;
  private final long retentionMs;
  private final ThreadPoolExecutor deleteExecutor;
  // Heartbeat token per job this manager currently owns, keyed by id. The scheduler renews it and a
  // worker reads it for the terminal CAS; refreshHeartbeats drops the entry once a peer reclaims.
  private final Map<Long, Long> ownedHeartbeats = new ConcurrentHashMap<>();

  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private ExecutorService workers;
  private ScheduledExecutorService scheduler;

  /**
   * Creates an async cleanup manager.
   *
   * @param store the cleanup job store backed by the entity store's relational backend
   * @param config Iceberg REST server config
   */
  public IcebergCleanupManager(IcebergCleanupJobStore store, IcebergConfig config) {
    this.store = store;
    this.workerThreads = config.get(IcebergConfig.ASYNC_CLEANUP_WORKER_THREADS);
    int deleteThreads = config.get(IcebergConfig.ASYNC_CLEANUP_DELETE_THREADS);
    this.deleteBatchSize = config.get(IcebergConfig.ASYNC_CLEANUP_DELETE_BATCH_SIZE);
    this.pollIntervalMs = config.get(IcebergConfig.ASYNC_CLEANUP_POLL_INTERVAL_SECS) * 1000L;
    this.heartbeatTimeoutMs =
        config.get(IcebergConfig.ASYNC_CLEANUP_HEARTBEAT_TIMEOUT_SECS) * 1000L;
    this.maxAttempts = config.get(IcebergConfig.ASYNC_CLEANUP_MAX_ATTEMPTS);
    this.retentionMs = config.get(IcebergConfig.ASYNC_CLEANUP_RETENTION_HOURS) * 3_600_000L;
    // Scan more candidates than workers so a claim that loses its CAS still has other rows to try
    // in the same poll. workerThreads * 4 gives that headroom; the floor of 8 keeps the window
    // useful when only one or two worker threads are configured.
    this.candidateWindow = Math.max(8, workerThreads * 4);
    this.deleteExecutor =
        new ThreadPoolExecutor(
            deleteThreads,
            deleteThreads,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(deleteThreads * 4),
            daemon("iceberg-cleanup-delete"),
            new ThreadPoolExecutor.CallerRunsPolicy());
  }

  /**
   * Persists a new cleanup job.
   *
   * @param job job to persist
   * @return generated id
   */
  public long addJob(IcebergCleanupJob job) {
    return store.addJob(job);
  }

  /**
   * Checks whether an unfinished cleanup job occupies a table identifier.
   *
   * @param catalogId globally unique id of the owning catalog
   * @param namespace table namespace
   * @param table table name
   * @return true iff a PENDING or RUNNING job exists for the identifier
   */
  public boolean isNameOccupied(long catalogId, String namespace, String table) {
    return store.findUnfinishedJobId(catalogId, namespace, table).isPresent();
  }

  /** Starts worker threads and the heartbeat/prune scheduler. */
  public void start() {
    if (closed.get()) {
      throw new IllegalStateException("Iceberg cleanup manager is already closed");
    }

    // compareAndSet keeps concurrent or repeated start() calls from each allocating a pool.
    if (!running.compareAndSet(false, true)) {
      return;
    }

    // We submit exactly workerThreads loops, so the queue is never used; it is bounded only to
    // avoid Executors.newFixedThreadPool's unbounded queue.
    this.workers =
        new ThreadPoolExecutor(
            workerThreads,
            workerThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(workerThreads),
            daemon("iceberg-cleanup-worker"));
    for (int i = 0; i < workerThreads; i++) {
      workers.submit(this::workerLoop);
    }

    // One scheduler thread runs both periodic tasks: heartbeat renewal and row pruning.
    this.scheduler = Executors.newScheduledThreadPool(1, daemon("iceberg-cleanup-heartbeat-prune"));
    long heartbeatIntervalMs = heartbeatTimeoutMs / 3L;
    scheduler.scheduleAtFixedRate(
        this::refreshHeartbeats, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    scheduler.scheduleAtFixedRate(this::prune, 1L, 1L, TimeUnit.HOURS);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    running.set(false);
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (workers != null) {
      workers.shutdownNow();
      awaitTermination(workers);
    }
    deleteExecutor.shutdownNow();
    // Wait for in-flight delete batches to observe the interrupt and stop, matching the workers
    // above, so close() does not return while file deletions are still running on a dying pool.
    awaitTermination(deleteExecutor);
    // The job store is backed by the entity store's shared relational backend, which owns the
    // connection pool lifecycle, so there is nothing to close here.
  }

  void cleanupFiles(FileIO io, String metadataLocation) {
    TableMetadata metadata;
    try {
      metadata = TableMetadataParser.read(io, metadataLocation);
    } catch (NotFoundException metadataAlreadyGone) {
      // A missing root metadata.json means the table is already gone. Since we delete it last, its
      // absence proves every file under it was deleted first, so this is the one NotFoundException
      // we treat as success. Return and let runJob mark the job SUCCEEDED.
      LOG.info("Cleanup metadata {} already absent; treating as done", metadataLocation);
      return;
    }

    Table table = new BaseTable(new StaticTableOperations(metadata, io), "async-cleanup");

    // Delete children before parents, root metadata.json last. Each deleteAll blocks until its
    // level is gone, so a crash always leaves the root (and the manifests above any surviving file)
    // readable for a retry to rebuild from. Deleting a parent first would orphan its children.
    //
    // Data files are the only huge level, so they are streamed and deleted one manifest at a time
    // rather than all collected first; only the smaller manifest/list/metadata paths are held.
    Set<String> manifests = new LinkedHashSet<>();
    deleteDataFiles(io, metadata, manifests);
    deleteAll(io, manifests);
    deleteAll(io, ReachableFileUtil.manifestListLocations(table));
    deleteAll(io, ReachableFileUtil.statisticsFilesLocations(table));

    // metadataFileLocations includes the current metadata.json; drop it so it is deleted last.
    Set<String> ancestorMetadata =
        new LinkedHashSet<>(ReachableFileUtil.metadataFileLocations(table, true));
    ancestorMetadata.remove(metadataLocation);
    deleteAll(io, ancestorMetadata);
    deleteAll(io, Collections.singletonList(metadataLocation));
  }

  void deleteAll(FileIO io, Iterable<String> files) {
    // Callers pass one manifest's files at a time (or a small fixed list), so futures stay small;
    // CallerRunsPolicy on deleteExecutor also throttles submission when the pool is saturated.
    List<Future<?>> futures = new ArrayList<>();
    Iterators.partition(files.iterator(), deleteBatchSize)
        .forEachRemaining(
            batch ->
                futures.add(
                    deleteExecutor.submit(
                        () -> CatalogUtil.deleteFiles(io, batch, "cleanup", true))));

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted during bulk delete", e);
      } catch (ExecutionException e) {
        if (Throwables.getCausalChain(e).stream().anyMatch(NotFoundException.class::isInstance)) {
          LOG.debug("Ignoring already-deleted file during async cleanup", e);
          continue;
        }
        throw new RuntimeException("Bulk delete batch failed", e);
      }
    }
  }

  private void workerLoop() {
    while (running.get()) {
      try {
        long now = System.currentTimeMillis();
        Optional<IcebergCleanupJob> job =
            store.takePendingJob(now, heartbeatTimeoutMs, candidateWindow);
        if (job.isEmpty()) {
          sleep(pollIntervalMs);
          continue;
        }

        ownedHeartbeats.put(job.get().id(), now);
        runJob(job.get());
      } catch (Throwable t) {
        // The loop is submitted once, so if it exits the worker is gone for good. Catch everything
        // (including Errors) so a fault only backs off instead of killing the worker.
        if (t instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        LOG.warn("Cleanup worker loop hit an unexpected error; backing off", t);
        sleep(pollIntervalMs);
      }
    }
  }

  private void runJob(IcebergCleanupJob job) {
    long id = job.id();
    // try-with-resources so the per-job FileIO (which may hold an S3 client / connection pool) is
    // closed on every path: success, transient failure, and the early return inside cleanupFiles.
    try (FileIO io = CatalogUtil.loadFileIO(job.fileIOImpl(), job.fileIOProperties(), null)) {
      cleanupFiles(io, job.metadataLocation());
      finishJob(id, heartbeat -> store.markSucceeded(id, heartbeat));
    } catch (RuntimeException e) {
      LOG.warn("Cleanup job {} failed transiently; will retry", id, e);
      finishJob(id, heartbeat -> store.recordFailure(id, e.getMessage(), maxAttempts, heartbeat));
    } finally {
      ownedHeartbeats.remove(id);
    }
  }

  // markSucceeded/recordFailure CAS on the heartbeat token, so a worker whose lease a peer
  // reclaimed cannot overwrite the job the peer now owns. A null token means a refresh already
  // saw the takeover, so we skip. A failed CAS just leaves the row RUNNING to be reclaimed and
  // re-run (which finds the files gone and succeeds); we log it so the reclaim is observable.
  void finishJob(long id, LongPredicate terminalUpdate) {
    Long heartbeat = ownedHeartbeats.get(id);
    if (heartbeat != null && !terminalUpdate.test(heartbeat)) {
      LOG.warn("Could not finish cleanup job {}; it will be reclaimed and re-run", id);
    }
  }

  void refreshHeartbeats() {
    long now = System.currentTimeMillis();
    for (Map.Entry<Long, Long> entry : new ArrayList<>(ownedHeartbeats.entrySet())) {
      long id = entry.getKey();
      try {
        long previousHeartbeat = entry.getValue();
        ownedHeartbeats.put(id, now);
        if (!store.heartbeat(id, previousHeartbeat, now)) {
          LOG.warn("Lost ownership of cleanup job {}", id);
          ownedHeartbeats.remove(id, now);
        }
      } catch (Throwable t) {
        ownedHeartbeats.replace(id, now, entry.getValue());
        // scheduleAtFixedRate stops a task forever if it throws, so never let one escape: a bad job
        // must not stop heartbeat renewal for the whole process.
        LOG.warn("Heartbeat update failed for job {}", id, t);
      }
    }
  }

  private void prune() {
    try {
      store.deleteFinishedJobsByLegacyTimeline(System.currentTimeMillis() - retentionMs);
    } catch (Throwable t) {
      // As above: don't let a throw stop the recurring prune task.
      LOG.warn("Cleanup-row pruning failed", t);
    }
  }

  // Streams each manifest's data files to deleteAll (one manifest's paths in memory at a time) and
  // collects the manifest paths into `manifests` for the caller to delete next.
  private void deleteDataFiles(FileIO io, TableMetadata metadata, Set<String> manifests) {
    for (Snapshot snapshot : metadata.snapshots()) {
      List<ManifestFile> snapshotManifests;
      try {
        snapshotManifests = snapshot.allManifests(io);
      } catch (NotFoundException manifestListGone) {
        // Manifest lists are deleted after everything under them, so a missing one means a prior
        // attempt already deleted this snapshot's files. Nothing left here; skip it.
        LOG.debug("Manifest list for snapshot {} already gone; skipping", snapshot.snapshotId());
        continue;
      }
      for (ManifestFile manifest : snapshotManifests) {
        if (!manifests.add(manifest.path())) {
          continue; // shared by several snapshots; its data files were already deleted
        }
        try (CloseableIterable<String> paths =
            ManifestFiles.readPaths(manifest, io, metadata.specsById())) {
          // deleteAll pulls this lazy iterable in batches, so only one batch is held at a time.
          deleteAll(io, paths);
        } catch (NotFoundException manifestGone) {
          // Manifests are deleted after their data files, so a missing one has no data files left.
          LOG.debug("Manifest {} already gone; skipping", manifest.path());
        } catch (Exception e) {
          throw new RuntimeException("Failed to read manifest " + manifest.path(), e);
        }
      }
    }
  }

  private static void awaitTermination(ExecutorService pool) {
    try {
      pool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory daemon(String name) {
    return runnable -> {
      Thread thread = new Thread(runnable, name);
      thread.setDaemon(true);
      return thread;
    };
  }
}
