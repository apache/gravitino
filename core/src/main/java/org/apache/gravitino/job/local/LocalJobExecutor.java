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

package org.apache.gravitino.job.local;

import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.DEFAULT_JOB_STATUS_KEEP_TIME_MS;
import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.DEFAULT_MAX_RUNNING_JOBS;
import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.DEFAULT_WAITING_QUEUE_SIZE;
import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.JOB_STATUS_KEEP_TIME_MS;
import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.MAX_RUNNING_JOBS;
import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.WAITING_QUEUE_SIZE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalJobExecutor implements JobExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(LocalJobExecutor.class);

  private static final String LOCAL_JOB_PREFIX = "local-job-";

  private static final long UNEXPIRED_TIME_IN_MS = -1L;

  // A random id of this executor instance, it is embedded in every job id submitted by this
  // instance, so that each Gravitino server only tracks the jobs it runs itself.
  private String executorId;

  private String ownedJobIdPrefix;

  private Map<String, String> configs;

  private BlockingQueue<Pair<String, JobTemplate>> waitingQueue;

  private ExecutorService jobExecutorService;

  private ExecutorService jobPollingExecutorService;

  // The job status map to keep track of the status of each job. In the meantime, the job status
  // will be stored in the entity store, so we will clean the finished, cancelled and failed jobs
  // from the map periodically to save the memory.
  private Map<String, Pair<JobHandle.Status, Long>> jobStatus;
  private final Object lock = new Object();

  private long jobStatusKeepTimeInMs;
  private ScheduledExecutorService jobStatusCleanupExecutor;

  private volatile boolean finished = false;

  private Map<String, Process> runningProcesses;

  // The working directory of each job, used to locate its captured stdout/stderr files. Cleaned
  // up together with jobStatus so the two maps stay in sync.
  private Map<String, File> jobWorkingDirs;

  @Override
  public void initialize(Map<String, String> configs) {
    this.configs = configs;
    this.executorId = String.format("%08x", ThreadLocalRandom.current().nextInt());
    this.ownedJobIdPrefix = LOCAL_JOB_PREFIX + executorId + "-";
    LOG.info("Initializing local job executor with executor id {}", executorId);

    int waitingQueueSize =
        configs.containsKey(WAITING_QUEUE_SIZE)
            ? Integer.parseInt(configs.get(WAITING_QUEUE_SIZE))
            : DEFAULT_WAITING_QUEUE_SIZE;
    Preconditions.checkArgument(
        waitingQueueSize > 0,
        "Waiting queue size must be greater than 0, but got: %s",
        waitingQueueSize);

    this.waitingQueue = new LinkedBlockingQueue<>(waitingQueueSize);

    this.jobPollingExecutorService =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("LocalJobPollingExecutor-" + thread.getId());
              thread.setDaemon(true);
              return thread;
            });
    jobPollingExecutorService.submit(this::pollJob);

    int maxRunningJobs =
        configs.containsKey(MAX_RUNNING_JOBS)
            ? Integer.parseInt(configs.get(MAX_RUNNING_JOBS))
            : DEFAULT_MAX_RUNNING_JOBS;
    Preconditions.checkArgument(
        maxRunningJobs > 0, "Max running jobs must be greater than 0, but got: %s", maxRunningJobs);

    // With an unbounded queue, the pool never grows beyond its core size, so the core size must be
    // the max running jobs. Idle core threads are still allowed to time out.
    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(
            maxRunningJobs,
            maxRunningJobs,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("LocalJobExecutor-" + thread.getId());
              thread.setDaemon(true);
              return thread;
            });
    threadPoolExecutor.allowCoreThreadTimeOut(true);
    this.jobExecutorService = threadPoolExecutor;

    this.jobStatus = Maps.newHashMap();

    this.jobStatusKeepTimeInMs =
        configs.containsKey(JOB_STATUS_KEEP_TIME_MS)
            ? Long.parseLong(configs.get(JOB_STATUS_KEEP_TIME_MS))
            : DEFAULT_JOB_STATUS_KEEP_TIME_MS;
    Preconditions.checkArgument(
        jobStatusKeepTimeInMs > 0,
        "Job status keep time must be greater than 0, but got: %s",
        jobStatusKeepTimeInMs);

    this.jobStatusKeepTimeInMs = Math.max(jobStatusKeepTimeInMs, 10);
    long jobStatusCleanupIntervalInMs = jobStatusKeepTimeInMs / 10;
    this.jobStatusCleanupExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("LocalJobStatusCleanup-" + thread.getId());
              thread.setDaemon(true);
              return thread;
            });
    jobStatusCleanupExecutor.scheduleAtFixedRate(
        this::cleanupJobStatus,
        jobStatusCleanupIntervalInMs,
        jobStatusCleanupIntervalInMs,
        TimeUnit.MILLISECONDS);

    this.runningProcesses = Maps.newConcurrentMap();
    this.jobWorkingDirs = Maps.newConcurrentMap();

    // Spark is optional for the local job executor, so a missing Spark installation must not fail
    // the server startup. Warn early instead; Spark jobs will be rejected at submission.
    try {
      SparkProcessBuilder.resolveSparkSubmit(configs);
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "Spark jobs cannot be run by the local job executor and will be rejected: {}",
          e.getMessage());
    }
  }

  @Override
  public String submitJob(JobTemplate jobTemplate) {
    // Validate the job can be launched before queueing it, so that a misconfiguration is reported
    // to the caller directly instead of only failing the job asynchronously in the worker thread.
    if (jobTemplate instanceof SparkJobTemplate) {
      SparkProcessBuilder.resolveSparkSubmit(configs);
    }

    String newJobId = ownedJobIdPrefix + UUID.randomUUID();
    Pair<String, JobTemplate> jobPair = Pair.of(newJobId, jobTemplate);

    synchronized (lock) {
      // Add the job template to the waiting queue
      if (!waitingQueue.offer(jobPair)) {
        throw new IllegalStateException("Waiting queue is full, cannot submit job: " + jobTemplate);
      }

      jobStatus.put(newJobId, Pair.of(JobHandle.Status.QUEUED, UNEXPIRED_TIME_IN_MS));
      // Retain the working directory so output.log/error.log can be located later without
      // needing to keep the running Process object around after the job finishes.
      jobWorkingDirs.put(newJobId, LocalProcessBuilder.resolveWorkingDirectory(jobTemplate));
    }

    return newJobId;
  }

  @Override
  public JobHandle.Status getJobStatus(String jobId) throws NoSuchJobException {
    synchronized (lock) {
      if (!jobStatus.containsKey(jobId)) {
        throw new NoSuchJobException("No job found with ID: %s", jobId);
      }
      LOG.debug(
          "Get status {} and finished time {} for job {}",
          jobStatus.get(jobId).getLeft(),
          jobStatus.get(jobId).getRight(),
          jobId);
      return jobStatus.get(jobId).getLeft();
    }
  }

  @Override
  public void cancelJob(String jobId) throws NoSuchJobException {
    synchronized (lock) {
      if (!jobStatus.containsKey(jobId)) {
        throw new NoSuchJobException("No job found with ID: %s", jobId);
      }

      Pair<JobHandle.Status, Long> statusPair = jobStatus.get(jobId);
      if (statusPair.getLeft() == JobHandle.Status.SUCCEEDED
          || statusPair.getLeft() == JobHandle.Status.FAILED
          || statusPair.getLeft() == JobHandle.Status.CANCELLED) {
        LOG.warn("Job {} is already completed or cancelled, no action taken", jobId);
        return;
      }

      if (statusPair.getLeft() == JobHandle.Status.CANCELLING) {
        LOG.warn("Job {} is already being cancelled, no action taken", jobId);
        return;
      }

      // If the job is queued.
      if (statusPair.getLeft() == JobHandle.Status.QUEUED) {
        waitingQueue.removeIf(p -> p.getLeft().equals(jobId));
        jobStatus.put(jobId, Pair.of(JobHandle.Status.CANCELLED, System.currentTimeMillis()));
        LOG.info("Job {} is cancelled from the waiting queue", jobId);
        return;
      }

      if (statusPair.getLeft() == JobHandle.Status.STARTED) {
        Process process = runningProcesses.get(jobId);
        if (process != null) {
          process.destroy();
        }
        LOG.info("Job {} is cancelling while running", jobId);
        jobStatus.put(jobId, Pair.of(JobHandle.Status.CANCELLING, UNEXPIRED_TIME_IN_MS));
      }
    }
  }

  @Override
  public boolean ownsJob(String jobId) {
    return jobId != null && jobId.startsWith(ownedJobIdPrefix);
  }

  @Override
  public boolean isJobStateNodeLocal() {
    return true;
  }

  @Override
  public List<String> getJobStdout(String jobId, int maxLines, int maxBytes) {
    return getJobOutput(jobId, LocalProcessBuilder.STDOUT_FILE_NAME, maxLines, maxBytes);
  }

  @Override
  public List<String> getJobStderr(String jobId, int maxLines, int maxBytes) {
    return getJobOutput(jobId, LocalProcessBuilder.STDERR_FILE_NAME, maxLines, maxBytes);
  }

  @Override
  public void close() throws IOException {
    // Mark the executor as finished to stop processing jobs
    this.finished = true;
    jobPollingExecutorService.shutdownNow();

    if (!waitingQueue.isEmpty()) {
      LOG.warn(
          "There are still {} jobs in the waiting queue, they will not be processed.",
          waitingQueue.size());
      waitingQueue.clear();
    }

    // Stop the running jobs
    runningProcesses.forEach(
        (key, process) -> {
          if (process != null) {
            process.destroy();
          }
        });
    runningProcesses.clear();

    jobExecutorService.shutdownNow();

    // Stop the job status cleanup executor
    jobStatusCleanupExecutor.shutdownNow();
    jobStatus.clear();
    jobWorkingDirs.clear();
  }

  public void runJob(Pair<String, JobTemplate> jobPair) {
    try {
      String jobId = jobPair.getLeft();
      JobTemplate jobTemplate = jobPair.getRight();

      Process process;
      synchronized (lock) {
        // This happens when the job is cancelled before it starts.
        Pair<JobHandle.Status, Long> statusPair = jobStatus.get(jobId);
        if (statusPair == null || statusPair.getLeft() != JobHandle.Status.QUEUED) {
          LOG.warn("Job {} is not in QUEUED state, cannot start it", jobId);
          return;
        }

        LocalProcessBuilder processBuilder = LocalProcessBuilder.create(jobTemplate, configs);
        process = processBuilder.start();
        runningProcesses.put(jobId, process);
        jobStatus.put(jobId, Pair.of(JobHandle.Status.STARTED, UNEXPIRED_TIME_IN_MS));
      }

      LOG.info("Starting job: {}", jobId);

      int exitCode = process.waitFor();
      if (exitCode == 0) {
        LOG.info("Job {} completed successfully", jobId);
        synchronized (lock) {
          jobStatus.put(jobId, Pair.of(JobHandle.Status.SUCCEEDED, System.currentTimeMillis()));
        }
      } else {
        synchronized (lock) {
          JobHandle.Status oldStatus = jobStatus.get(jobId).getLeft();
          if (oldStatus == JobHandle.Status.CANCELLING) {
            LOG.info("Job {} was cancelled while running with exit code: {}", jobId, exitCode);
            jobStatus.put(jobId, Pair.of(JobHandle.Status.CANCELLED, System.currentTimeMillis()));
          } else if (oldStatus == JobHandle.Status.STARTED) {
            LOG.warn("Job {} failed after starting with exit code: {}", jobId, exitCode);
            jobStatus.put(jobId, Pair.of(JobHandle.Status.FAILED, System.currentTimeMillis()));
          }
        }
      }

    } catch (Exception e) {
      LOG.error("Error while executing job", e);
      // If an error occurs, we should mark the job as failed
      synchronized (lock) {
        String jobId = jobPair.getLeft();
        jobStatus.put(jobId, Pair.of(JobHandle.Status.FAILED, System.currentTimeMillis()));
      }
    }

    runningProcesses.remove(jobPair.getLeft());
  }

  public void pollJob() {
    while (!finished) {
      try {
        Pair<String, JobTemplate> jobPair = waitingQueue.poll(3000, TimeUnit.MILLISECONDS);
        if (jobPair == null) {
          // If no job is available, continue to the next iteration
          continue;
        }

        jobExecutorService.submit(() -> runJob(jobPair));

      } catch (InterruptedException e) {
        LOG.warn("Polling job interrupted");
        finished = true;
      }
    }
  }

  @VisibleForTesting
  String executorId() {
    return executorId;
  }

  @VisibleForTesting
  void cleanupJobStatus() {
    long currentTime = System.currentTimeMillis();

    synchronized (lock) {
      jobStatus
          .entrySet()
          .removeIf(
              entry -> {
                boolean expired =
                    entry.getValue().getRight() != UNEXPIRED_TIME_IN_MS
                        && (currentTime - entry.getValue().getRight()) >= jobStatusKeepTimeInMs;
                if (expired) {
                  jobWorkingDirs.remove(entry.getKey());
                }
                return expired;
              });
    }
  }

  private List<String> getJobOutput(String jobId, String fileName, int maxLines, int maxBytes) {
    File workingDir = getWorkingDir(jobId);
    if (workingDir == null) {
      return ImmutableList.of();
    }
    return readLastLines(new File(workingDir, fileName), maxLines, maxBytes);
  }

  @Nullable
  private File getWorkingDir(String jobId) {
    return jobWorkingDirs.get(jobId);
  }

  private List<String> readLastLines(File file, int maxLines, int maxBytes) {
    if (!file.exists()) {
      // The job hasn't started (or hasn't produced this stream) yet.
      return ImmutableList.of();
    }

    long fileLength = file.length();
    int windowSize = (int) Math.min(fileLength, maxBytes);
    long startOffset = fileLength - windowSize;

    // Read one extra leading byte (when available) so we can tell whether the window's first
    // line is already complete - i.e. the file byte immediately before the window is itself a
    // line terminator - rather than always assuming it's a partial line and discarding it.
    long readOffset = Math.max(0, startOffset - 1);
    byte[] probeWindow = new byte[(int) (fileLength - readOffset)];
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      raf.seek(readOffset);
      raf.readFully(probeWindow);
    } catch (FileNotFoundException e) {
      // The file existed at the check above but is gone now - most likely
      // JobManager#cleanUpStagingDirs deleted the staging directory concurrently with this call.
      // That's the same "output no longer available" situation the file-doesn't-exist check above
      // handles, not a real I/O failure, so it must degrade to empty output the same way.
      return ImmutableList.of();
    } catch (IOException e) {
      // Any other I/O failure while reading an existing file is unexpected and must not be
      // silently reported as "no output" - that would be actively misleading for the debugging
      // use case this method exists for.
      throw new RuntimeException("Failed to read job output file: " + file, e);
    }

    boolean windowStartsAtLineBoundary = startOffset == 0 || probeWindow[0] == '\n';
    int contentStart = startOffset == 0 ? 0 : 1;
    // The window may start mid-character if the file byte at contentStart happens to be a UTF-8
    // continuation byte - skip forward to the next character boundary so the decoded content
    // never begins with a corrupted replacement character. A '\n' byte never appears inside a
    // multi-byte UTF-8 character, so this can't skip past a real line boundary.
    while (contentStart < probeWindow.length && (probeWindow[contentStart] & 0xC0) == 0x80) {
      contentStart++;
    }

    String content =
        new String(
            probeWindow, contentStart, probeWindow.length - contentStart, StandardCharsets.UTF_8);
    if (!windowStartsAtLineBoundary) {
      // The window starts mid-file and the preceding byte isn't a line terminator, so its first
      // line may be a partial line whose true beginning fell outside the window - drop up to and
      // including the first newline, but only when something actually follows it. If the only
      // newline in the window is its very last character, that newline terminates the single
      // oversized line the window is entirely made of, rather than starting a subsequent one -
      // dropping through it would discard the whole line instead of truncating it. Keep the
      // content as-is in that case (and when no newline is found at all), since a truncated line
      // is more useful for debugging than silently returning nothing.
      int firstNewline = content.indexOf('\n');
      if (firstNewline >= 0 && firstNewline < content.length() - 1) {
        content = content.substring(firstNewline + 1);
      }
    }

    if (content.isEmpty()) {
      return ImmutableList.of();
    }

    // Recognize both LF and CRLF line endings, so output captured with Windows-style line
    // endings doesn't leave a dangling '\r' on every returned line.
    List<String> lines = Splitter.onPattern("\r\n|\n").splitToList(content);
    if (content.endsWith("\n")) {
      // A trailing separator produces a spurious empty trailing element - drop it so a
      // completed line isn't followed by a phantom blank one.
      lines = lines.subList(0, lines.size() - 1);
    }

    int fromIndex = Math.max(0, lines.size() - maxLines);
    return ImmutableList.copyOf(lines.subList(fromIndex, lines.size()));
  }
}
