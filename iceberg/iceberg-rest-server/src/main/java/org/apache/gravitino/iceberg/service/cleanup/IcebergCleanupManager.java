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

import com.google.common.collect.Iterators;
import java.util.ArrayList;
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
  private final Map<Long, Long> ownedHeartbeats = new ConcurrentHashMap<>();

  private volatile boolean running;
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
    if (running) {
      return;
    }

    running = true;
    // Exactly workerThreads long-running loops are submitted below, so the bounded queue is never
    // actually used; it is declared explicitly to avoid Executors.newFixedThreadPool's unbounded
    // LinkedBlockingQueue.
    workers =
        new ThreadPoolExecutor(
            workerThreads,
            workerThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(workerThreads),
            daemon("iceberg-cleanup-worker"));
    scheduler = Executors.newScheduledThreadPool(1, daemon("iceberg-cleanup-scheduler"));
    for (int i = 0; i < workerThreads; i++) {
      workers.submit(this::workerLoop);
    }

    long heartbeatIntervalMs = Math.max(1L, heartbeatTimeoutMs / 3L);
    scheduler.scheduleAtFixedRate(
        this::refreshHeartbeats, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    scheduler.scheduleAtFixedRate(this::prune, 1L, 1L, TimeUnit.HOURS);
  }

  @Override
  public void close() {
    running = false;
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (workers != null) {
      workers.shutdownNow();
      try {
        workers.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    deleteExecutor.shutdownNow();
    // The job store is backed by the entity store's shared relational backend, which owns the
    // connection pool lifecycle, so there is nothing to close here.
  }

  void cleanupFiles(FileIO io, String metadataLocation) {
    TableMetadata metadata;
    try {
      metadata = TableMetadataParser.read(io, metadataLocation);
    } catch (NotFoundException metadataAlreadyGone) {
      // The root metadata.json pointer is the ONLY file whose absence means "the table is already
      // gone", so this is the single place a NotFoundException is treated as success: a prior
      // attempt finished deleting the table before its row was marked, or the table was never
      // fully written. Downstream, enumeration (reachableFiles) and bulk deletion (deleteAll)
      // tolerate already-deleted files locally instead of letting a NotFoundException escape, so
      // it never reaches here from anywhere but this read. There is nothing left to clean up;
      // return so runJob marks the job SUCCEEDED.
      LOG.info(
          "Cleanup metadata {} already absent; treating as completed",
          metadataLocation,
          metadataAlreadyGone);
      return;
    }
    deleteAll(io, reachableFiles(io, metadata));
  }

  void deleteAll(FileIO io, Iterable<String> files) {
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
        if (hasCause(e, NotFoundException.class)) {
          LOG.debug("Ignoring already-deleted file during async cleanup", e);
          continue;
        }
        throw new RuntimeException("Bulk delete batch failed", e);
      }
    }
  }

  private static boolean hasCause(Throwable throwable, Class<? extends Throwable> type) {
    Throwable current = throwable;
    while (current != null) {
      if (type.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private void workerLoop() {
    while (running) {
      try {
        long now = System.currentTimeMillis();
        Optional<IcebergCleanupJob> job =
            store.takePendingJob(now, heartbeatTimeoutMs, candidateWindow);
        if (!job.isPresent()) {
          sleep(pollIntervalMs);
          continue;
        }

        ownedHeartbeats.put(job.get().id(), now);
        runJob(job.get());
      } catch (Throwable t) {
        // A worker task is submitted exactly once in start(), so if this loop ever exits the
        // thread is gone for good and the pool shrinks permanently. Catch every Throwable --
        // including Errors and anything takePendingJob/runJob may surface -- so a fault only backs
        // the loop off instead of killing the worker. Re-assert the interrupt flag so shutdown
        // still unwinds promptly.
        if (t instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        LOG.warn("Cleanup worker loop hit an unexpected error; backing off", t);
        sleep(pollIntervalMs);
      }
    }
  }

  private void runJob(IcebergCleanupJob job) {
    try {
      FileIO io = CatalogUtil.loadFileIO(job.fileIOImpl(), job.fileIOProperties(), null);
      // cleanupFiles swallows the only legitimate already-gone signal (a missing metadata.json)
      // internally, so reaching this line means the table's files were enumerated and deleted.
      cleanupFiles(io, job.metadataLocation());
      store.markSucceeded(job.id());
    } catch (RuntimeException e) {
      LOG.warn("Cleanup job {} failed transiently; will retry", job.id(), e);
      store.recordFailure(job.id(), e.getMessage(), maxAttempts);
    } finally {
      ownedHeartbeats.remove(job.id());
    }
  }

  private void refreshHeartbeats() {
    long now = System.currentTimeMillis();
    List<Map.Entry<Long, Long>> heartbeats = new ArrayList<>(ownedHeartbeats.entrySet());
    for (Map.Entry<Long, Long> entry : heartbeats) {
      try {
        if (store.heartbeat(entry.getKey(), entry.getValue(), now)) {
          ownedHeartbeats.put(entry.getKey(), now);
        } else {
          LOG.warn("Lost ownership of cleanup job {}", entry.getKey());
          ownedHeartbeats.remove(entry.getKey());
        }
      } catch (Throwable t) {
        // scheduleAtFixedRate suppresses all future runs if a task ever throws, which would stop
        // heartbeat renewal for the whole process. Swallow per-job faults (including Errors) so one
        // bad job neither halts this pass nor the recurring task.
        LOG.warn("Heartbeat update failed for job {}", entry.getKey(), t);
      }
    }
  }

  private void prune() {
    try {
      store.deleteFinishedJobsByLegacyTimeline(System.currentTimeMillis() - retentionMs);
    } catch (Throwable t) {
      // As above: never let a Throwable escape a scheduleAtFixedRate task, or pruning silently
      // stops for the life of the process and finished rows accumulate without bound.
      LOG.warn("Cleanup-row pruning failed", t);
    }
  }

  private Iterable<String> reachableFiles(FileIO io, TableMetadata metadata) {
    Table table = new BaseTable(new StaticTableOperations(metadata, io), "async-cleanup");
    Set<String> files = new LinkedHashSet<>();
    files.addAll(ReachableFileUtil.metadataFileLocations(table, true));
    files.addAll(ReachableFileUtil.manifestListLocations(table));
    files.addAll(ReachableFileUtil.statisticsFilesLocations(table));

    for (Snapshot snapshot : metadata.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        files.add(manifest.path());
        try (CloseableIterable<String> paths =
            ManifestFiles.readPaths(manifest, io, metadata.specsById())) {
          for (String path : paths) {
            files.add(path);
          }
        } catch (NotFoundException alreadyDeleted) {
          // A concurrent worker (e.g. one that reclaimed this job after a heartbeat timeout) may
          // already have deleted this manifest; its data files are gone with it. Skip it and keep
          // enumerating the rest rather than surfacing the NotFoundException -- only the root
          // metadata.json read in cleanupFiles treats a NotFoundException as "table already gone".
          // This mirrors deleteAll tolerating already-deleted files and keeps cleanup idempotent
          // under double processing.
          LOG.debug(
              "Manifest {} already deleted during async cleanup; skipping",
              manifest.path(),
              alreadyDeleted);
        } catch (Exception e) {
          throw new RuntimeException("Failed to read manifest " + manifest.path(), e);
        }
      }
    }
    return files;
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
