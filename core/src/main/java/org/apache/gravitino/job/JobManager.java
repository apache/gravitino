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

package org.apache.gravitino.job;

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.FileFetcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobManager implements JobOperationDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(JobManager.class);

  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{([\\w.-]+)\\}\\}");

  private static final String JOB_STAGING_DIR =
      File.separator
          + "%s"
          + File.separator
          + "%s"
          + File.separator
          + JobHandle.JOB_ID_PREFIX
          + "%s";

  private static final long JOB_STAGING_DIR_CLEANUP_MIN_TIME_IN_MS = 600 * 1000L; // 10 minute

  private static final long JOB_STATUS_PULL_MIN_INTERVAL_IN_MS = 60 * 1000L; // 1 minute

  private static final int TIMEOUT_IN_MS = 30 * 1000; // 30 seconds

  private final EntityStore entityStore;

  private final File stagingDir;

  private final JobExecutor jobExecutor;

  private final IdGenerator idGenerator;

  private final long jobStagingDirKeepTimeInMs;

  private final int jobOutputMaxLines;

  private final int jobOutputMaxBytes;

  @VisibleForTesting final ScheduledExecutorService cleanUpExecutor;

  @VisibleForTesting final ScheduledExecutorService statusPullExecutor;

  public JobManager(Config config, EntityStore entityStore, IdGenerator idGenerator) {
    this(config, entityStore, idGenerator, JobExecutorFactory.create(config));
  }

  @VisibleForTesting
  JobManager(
      Config config, EntityStore entityStore, IdGenerator idGenerator, JobExecutor jobExecutor) {
    this.entityStore = entityStore;
    this.jobExecutor = jobExecutor;
    this.idGenerator = idGenerator;

    String stagingDirPath = config.get(Configs.JOB_STAGING_DIR);
    this.stagingDir = new File(stagingDirPath);
    if (stagingDir.exists()) {
      if (!stagingDir.isDirectory()) {
        throw new IllegalArgumentException(
            String.format("Staging directory %s exists but is not a directory", stagingDirPath));
      }

      if (!(stagingDir.canExecute() && stagingDir.canRead() && stagingDir.canWrite())) {
        throw new IllegalArgumentException(
            String.format("Staging directory %s is not accessible", stagingDirPath));
      }
    } else {
      try {
        Files.createDirectories(stagingDir.toPath());
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Failed to create staging directory %s", stagingDirPath), e);
      }
    }

    this.jobStagingDirKeepTimeInMs = config.get(Configs.JOB_STAGING_DIR_KEEP_TIME_IN_MS);
    if (jobStagingDirKeepTimeInMs < JOB_STAGING_DIR_CLEANUP_MIN_TIME_IN_MS) {
      LOG.warn(
          "The job staging directory keep time is set to {} ms, the number is too small, "
              + "which will cause frequent cleanup, please set it to a value larger than {} if "
              + "you're not using it to do the test.",
          jobStagingDirKeepTimeInMs,
          JOB_STAGING_DIR_CLEANUP_MIN_TIME_IN_MS);
    }

    this.jobOutputMaxLines = config.get(Configs.JOB_OUTPUT_MAX_LINES);
    this.jobOutputMaxBytes = config.get(Configs.JOB_OUTPUT_MAX_BYTES);

    this.cleanUpExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "job-staging-dir-cleanup");
              thread.setDaemon(true);
              return thread;
            });
    long scheduleInterval = jobStagingDirKeepTimeInMs / 10;
    Preconditions.checkArgument(
        scheduleInterval != 0,
        "The schedule interval for "
            + "job staging directory cleanup cannot be zero, please set the job staging directory "
            + "keep time to a value larger than %s ms",
        JOB_STAGING_DIR_CLEANUP_MIN_TIME_IN_MS);

    cleanUpExecutor.scheduleAtFixedRate(
        this::cleanUpStagingDirs, scheduleInterval, scheduleInterval, TimeUnit.MILLISECONDS);

    long jobStatusPullIntervalInMs = config.get(Configs.JOB_STATUS_PULL_INTERVAL_IN_MS);
    if (jobStatusPullIntervalInMs < JOB_STATUS_PULL_MIN_INTERVAL_IN_MS) {
      LOG.warn(
          "The job status pull interval is set to {} ms, the number is too small, "
              + "which will cause frequent job status pull from external job executor, please set "
              + "it to a value larger than {} if you're not using it to do the test.",
          jobStatusPullIntervalInMs,
          JOB_STATUS_PULL_MIN_INTERVAL_IN_MS);
    }
    this.statusPullExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "job-status-pull");
              thread.setDaemon(true);
              return thread;
            });
    statusPullExecutor.scheduleAtFixedRate(
        this::pullAndUpdateJobStatus,
        jobStatusPullIntervalInMs,
        jobStatusPullIntervalInMs,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public List<JobTemplateEntity> listJobTemplates(String metalake) {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    Namespace jobTemplateNs = NamespaceUtil.ofJobTemplate(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(jobTemplateNs.levels()),
        LockType.READ,
        () -> {
          try {
            return entityStore.list(
                jobTemplateNs, JobTemplateEntity.class, Entity.EntityType.JOB_TEMPLATE);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobTemplateIdent =
        NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateEntity.name());
    TreeLockUtils.doWithTreeLock(
        jobTemplateIdent,
        LockType.WRITE,
        () -> {
          try {
            entityStore.put(jobTemplateEntity, false /* overwrite */);
            return null;
          } catch (EntityAlreadyExistsException e) {
            throw new JobTemplateAlreadyExistsException(
                "Job template with name %s under metalake %s already exists",
                jobTemplateEntity.name(), metalake);
          } catch (NoSuchEntityException e) {
            throw new NoSuchMetalakeException(e, "Metalake %s does not exist", metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobTemplateIdent = NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName);
    return TreeLockUtils.doWithTreeLock(
        jobTemplateIdent,
        LockType.READ,
        () -> {
          try {
            return entityStore.get(
                jobTemplateIdent, Entity.EntityType.JOB_TEMPLATE, JobTemplateEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchJobTemplateException(
                "Job template with name %s under metalake %s does not exist",
                jobTemplateName, metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    List<JobEntity> jobs;
    try {
      jobs = listJobs(metalake, Optional.of(jobTemplateName));
    } catch (NoSuchJobTemplateException e) {
      // If the job template does not exist, we can safely return false.
      return false;
    }

    boolean hasActiveJobs = jobs.stream().anyMatch(job -> !isFinishedStatus(job.status()));
    if (hasActiveJobs) {
      throw new InUseException(
          "Job template %s under metalake %s has active jobs associated with it",
          jobTemplateName, metalake);
    }

    // Delete the job template entity as well as all the jobs associated with it.
    boolean deleted =
        TreeLockUtils.doWithTreeLock(
            NameIdentifier.of(NamespaceUtil.ofJobTemplate(metalake).levels()),
            LockType.WRITE,
            () -> {
              try {
                return entityStore.delete(
                    NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName),
                    Entity.EntityType.JOB_TEMPLATE);
              } catch (NonEmptyEntityException e) {
                throw new InUseException(
                    "Job template %s under metalake %s has active jobs associated with it",
                    jobTemplateName, metalake);
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            });
    if (!deleted) {
      return false;
    }

    // Only remove directories belonging to the observed jobs. A same-name template can be
    // recreated after the metadata transaction commits, so its parent directory is not ours to
    // delete.
    for (JobEntity job : jobs) {
      String jobStagingPath =
          stagingDir.getAbsolutePath()
              + String.format(JOB_STAGING_DIR, metalake, job.jobTemplateName(), job.id());
      try {
        FileUtils.deleteDirectory(new File(jobStagingPath));
      } catch (IOException e) {
        LOG.error("Failed to delete job staging directory: {}", jobStagingPath, e);
      }
    }

    return true;
  }

  @Override
  public JobTemplateEntity alterJobTemplate(
      String metalake, String jobTemplateName, JobTemplateChange... changes)
      throws NoSuchJobTemplateException, IllegalArgumentException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    Optional<String> newName =
        Arrays.stream(changes)
            .filter(c -> c instanceof JobTemplateChange.RenameJobTemplate)
            .map(c -> ((JobTemplateChange.RenameJobTemplate) c).getNewName())
            .reduce((first, second) -> second);

    NameIdentifier jobTemplateIdent = NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName);
    return TreeLockUtils.doWithTreeLock(
        jobTemplateIdent,
        LockType.READ, // Use READ lock because the update method in JobTemplateMetaService will
        // handle the update transactionally and update with a new version number. So we don't
        // have to use a WRITE lock here.
        () -> {
          try {
            return entityStore.update(
                jobTemplateIdent,
                JobTemplateEntity.class,
                Entity.EntityType.JOB_TEMPLATE,
                jobTemplateEntity ->
                    updateJobTemplateEntity(jobTemplateIdent, jobTemplateEntity, changes));
          } catch (NoSuchEntityException e) {
            throw new NoSuchJobTemplateException(
                "Job template with name %s under metalake %s does not exist",
                jobTemplateName, metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          } catch (EntityAlreadyExistsException e) {
            // If the EntityAlreadyExistsException is thrown, it means the new name already exists.
            // So there should be a rename change, and the new name should be present.
            throw new RuntimeException(
                String.format(
                    "Failed to rename job template from %s to %s under metalake %s, the new name "
                        + "already exists",
                    jobTemplateName, newName, metalake),
                e);
          }
        });
  }

  @Override
  public List<JobEntity> listJobs(String metalake, Optional<String> jobTemplateName)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    Namespace jobNs = NamespaceUtil.ofJob(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(jobNs.levels()),
        LockType.READ,
        () -> {
          try {
            // If jobTemplateName is present, check if the job template exists, will throw an
            // exception if the job template does not exist.
            jobTemplateName.ifPresent(s -> getJobTemplate(metalake, s));

            List<JobEntity> jobEntities;
            if (jobTemplateName.isPresent()) {
              NameIdentifier jobTemplateIdent =
                  NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName.get());

              // If jobTemplateName is present, we need to list the jobs associated with the job.
              // Using a mock namespace from job template identifier to get the jobs associated
              // with job template.
              String[] elements =
                  ArrayUtils.add(jobTemplateIdent.namespace().levels(), jobTemplateIdent.name());
              Namespace jobTemplateIdentNs = Namespace.of(elements);

              // Lock the job template to ensure no concurrent modifications/deletions
              jobEntities =
                  TreeLockUtils.doWithTreeLock(
                      jobTemplateIdent,
                      LockType.READ,
                      () ->
                          // List all the jobs associated with the job template
                          entityStore.list(
                              jobTemplateIdentNs, JobEntity.class, Entity.EntityType.JOB));
            } else {
              jobEntities = entityStore.list(jobNs, JobEntity.class, Entity.EntityType.JOB);
            }
            return jobEntities;

          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public JobEntity getJob(
      String metalake, String jobId, boolean includeOutput, Integer maxLines, Integer maxBytes)
      throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobIdent = NameIdentifierUtil.ofJob(metalake, jobId);
    JobEntity entity =
        TreeLockUtils.doWithTreeLock(
            jobIdent,
            LockType.READ,
            () -> {
              try {
                return entityStore.get(jobIdent, Entity.EntityType.JOB, JobEntity.class);
              } catch (NoSuchEntityException e) {
                throw new NoSuchJobException(
                    "Job with ID %s under metalake %s does not exist", jobId, metalake);
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            });

    if (!includeOutput) {
      return entity;
    }

    // maxLines/maxBytes are only meaningful (and only validated) when output is actually
    // requested - per the documented contract, they're ignored entirely when includeOutput is
    // false, so an invalid value must not fail a plain getJob call that never uses them.
    Preconditions.checkArgument(
        maxLines == null || maxLines > 0, "maxLines must be positive if specified");
    Preconditions.checkArgument(
        maxBytes == null || maxBytes > 0, "maxBytes must be positive if specified");

    // A caller-specified maxLines/maxBytes can only narrow the globally configured cap, never
    // widen it - the global configuration remains a hard upper bound on read cost/response size.
    int effectiveMaxLines = clampToGlobalMax(maxLines, jobOutputMaxLines);
    int effectiveMaxBytes = clampToGlobalMax(maxBytes, jobOutputMaxBytes);

    // The job entity's existence was already confirmed above via the entity store, which is the
    // durable source of truth. The executor's own bookkeeping for a job's output is best-effort
    // and can legitimately be unavailable while the entity still exists (e.g. LocalJobExecutor
    // can't reach the output of a job that ran on a server not sharing its staging directory),
    // so JobExecutor#getJobStdout/getJobStderr report a job unknown to the executor as
    // empty output rather than an error - it never means "job does not exist" at this point.
    List<String> stdout =
        jobExecutor.getJobStdout(entity.jobExecutionId(), effectiveMaxLines, effectiveMaxBytes);
    List<String> stderr =
        jobExecutor.getJobStderr(entity.jobExecutionId(), effectiveMaxLines, effectiveMaxBytes);
    if (stdout.isEmpty() && stderr.isEmpty() && LOG.isDebugEnabled()) {
      LOG.debug(
          "No output available for job {} under metalake {} - either it produced none, or the "
              + "job executor no longer has a record of it",
          jobId,
          metalake);
    }
    return entity.withOutput(stdout, stderr);
  }

  // A caller-specified value can only narrow the globally configured cap, never widen it -
  // null means "use the global default", and any non-null value is clamped to at most that
  // default so the global configuration always remains a hard upper bound.
  private static int clampToGlobalMax(Integer requested, int globalMax) {
    return requested == null ? globalMax : Math.min(requested, globalMax);
  }

  @Override
  public JobEntity runJob(String metalake, String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    // Check if the job template exists, will throw NoSuchJobTemplateException if it does not exist.
    JobTemplateEntity jobTemplateEntity = getJobTemplate(metalake, jobTemplateName);

    // Create staging directory.
    long jobId = idGenerator.nextId();
    String jobStagingPath =
        stagingDir.getAbsolutePath()
            + String.format(JOB_STAGING_DIR, metalake, jobTemplateName, jobId);
    File jobStagingDir = new File(jobStagingPath);
    try {
      Files.createDirectories(jobStagingDir.toPath());
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to create staging directory %s for job %s", jobStagingDir, jobId),
          e);
    }

    // Create a JobTemplate by replacing the template parameters with the jobConf values, and
    // also downloading any necessary files from the URIs specified in the job template.
    JobTemplate jobTemplate = createRuntimeJobTemplate(jobTemplateEntity, jobConf, jobStagingDir);

    // Serialize the resolved (placeholder-replaced) job template so callers can later see exactly
    // what was submitted for execution, not just the original template. This is done before
    // submission so that a serialization failure never leaves a job running on the executor
    // without a corresponding JobEntity.
    JobTemplateDTO runtimeJobTemplateDTO =
        DTOConverters.toDTO(jobTemplate, DTOConverters.toDTO(jobTemplateEntity.auditInfo()));
    String runtimeJobTemplateJson;
    try {
      runtimeJobTemplateJson = JsonUtils.anyFieldMapper().writeValueAsString(runtimeJobTemplateDTO);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize the runtime job template", e);
    }

    // Submit the job template to the job executor
    String jobExecutionId;
    try {
      jobExecutionId = jobExecutor.submitJob(jobTemplate);
    } catch (IllegalArgumentException e) {
      // The job executor rejects the job because it cannot be launched, for example, a required
      // configuration is missing. Rethrow it as is so the caller gets the original reason.
      deleteStagingDirOfUnsubmittedJob(jobStagingDir, jobId);
      throw e;
    } catch (Exception e) {
      deleteStagingDirOfUnsubmittedJob(jobStagingDir, jobId);
      throw new RuntimeException(
          String.format("Failed to submit job template %s for execution", jobTemplate), e);
    }

    // Create a new JobEntity to represent the job
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(jobId)
            .withJobExecutionId(jobExecutionId)
            .withJobTemplateName(jobTemplateName)
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            // A newly submitted job is queued, not started or finished yet.
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .build();

    try {
      entityStore.put(jobEntity, false /* overwrite */);
    } catch (NoSuchEntityException e) {
      LOG.error(
          "Job {} was submitted as execution {} but could not be registered because its template "
              + "{} or metalake {} no longer exists",
          jobEntity.name(),
          jobExecutionId,
          jobTemplateName,
          metalake,
          e);
      throw new NoSuchJobTemplateException(
          e,
          "Job template with name %s under metalake %s does not exist",
          jobTemplateName,
          metalake);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register the job entity " + jobEntity, e);
    }

    return jobEntity;
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    // Retrieve the job entity, will throw NoSuchJobException if the job does not exist.
    JobEntity jobEntity = getJob(metalake, jobId, false);

    if (jobEntity.status() == JobHandle.Status.CANCELLING
        || jobEntity.status() == JobHandle.Status.CANCELLED
        || jobEntity.status() == JobHandle.Status.SUCCEEDED
        || jobEntity.status() == JobHandle.Status.FAILED) {
      // If the job is already cancelling, cancelled, succeeded, or failed, we do not need to cancel
      // it again.
      return jobEntity;
    }

    // Cancel the job using the job executor if this server owns the job. Otherwise, the job runs
    // on another server, so only mark it as CANCELLING below, and the owning server cancels it
    // when it pulls the job status next time.
    if (jobExecutor.ownsJob(jobEntity.jobExecutionId())) {
      try {
        jobExecutor.cancelJob(jobEntity.jobExecutionId());
      } catch (NoSuchJobException e) {
        // The job is lost by the job executor, mark it as CANCELLING and the status pull will
        // settle it as CANCELLED.
        LOG.warn(
            "Job {} with execution id {} under metalake {} is not found in the job executor, "
                + "marking it as CANCELLING",
            jobId,
            jobEntity.jobExecutionId(),
            metalake);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to cancel job with ID %s under metalake %s", jobId, metalake), e);
      }
    } else {
      LOG.info(
          "Job {} with execution id {} under metalake {} is owned by another job executor "
              + "instance, marking it as CANCELLING for the owner to cancel it",
          jobId,
          jobEntity.jobExecutionId(),
          metalake);
    }

    // Update the job status to CANCELING
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofJob(metalake, jobId),
        LockType.WRITE,
        () -> {
          try {
            // entityStore.update() re-fetches the latest entity itself right before applying the
            // updater, rather than reusing the snapshot taken before the (potentially slow)
            // external cancel call above - a concurrent status poll could have persisted a real
            // startedAt/finishedAt in that gap, and carrying forward the stale snapshot would
            // clobber it back to the sentinel.
            return entityStore.update(
                NameIdentifierUtil.ofJob(metalake, jobId),
                JobEntity.class,
                Entity.EntityType.JOB,
                this::toCancellingJobEntity);
          } catch (NoSuchEntityException e) {
            throw new NoSuchJobException(
                "Job with ID %s under metalake %s does not exist, this could be due to the job "
                    + "not existing or being deleted concurrently.",
                jobId, metalake);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Failed to update job entity for job %s to CANCELING status", jobId),
                e);
          }
        });
  }

  private JobEntity toCancellingJobEntity(JobEntity latestJobEntity) {
    // The external cancel call happens before this locked update, so a concurrent status poll
    // can persist a terminal status (or another cancelJob() call can already have moved the job
    // to CANCELLING) in the gap between the pre-cancel snapshot and this re-fetch. Never regress
    // the latest entity out of a terminal state, or overwrite an already-CANCELLING one.
    if (isFinishedStatus(latestJobEntity.status())
        || latestJobEntity.status() == JobHandle.Status.CANCELLING) {
      return latestJobEntity;
    }

    return JobEntity.builder()
        .withId(latestJobEntity.id())
        .withJobExecutionId(latestJobEntity.jobExecutionId())
        .withJobTemplateName(latestJobEntity.jobTemplateName())
        .withStatus(JobHandle.Status.CANCELLING)
        .withNamespace(latestJobEntity.namespace())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(latestJobEntity.auditInfo().creator())
                .withCreateTime(latestJobEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        // CANCELLING is not a terminal state; carry forward whatever startedAt/finishedAt the
        // job already had.
        .withStartedAt(latestJobEntity.startedAt())
        .withFinishedAt(latestJobEntity.finishedAt())
        // The runtime job template is fixed at job creation and never changes.
        .withRuntimeJobTemplate(latestJobEntity.runtimeJobTemplate())
        .build();
  }

  @Override
  public void close() throws IOException {
    try {
      jobExecutor.close();
    } finally {
      statusPullExecutor.shutdownNow();
      cleanUpExecutor.shutdownNow();
    }
  }

  @VisibleForTesting
  void pullAndUpdateJobStatus() {
    List<String> metalakes = MetalakeManager.listInUseMetalakes(entityStore);
    for (String metalake : metalakes) {
      // This unnecessary list all the jobs, we need to improve the code to only list the active
      // jobs.
      List<JobEntity> activeJobs =
          listJobs(metalake, Optional.empty()).stream()
              .filter(
                  job ->
                      job.status() == JobHandle.Status.QUEUED
                          || job.status() == JobHandle.Status.STARTED
                          || job.status() == JobHandle.Status.CANCELLING)
              .toList();

      activeJobs.forEach(
          job -> {
            // Only the job executor instance owning the job can query its status. The jobs
            // owned by other servers are skipped, and the jobs left behind by a server that has
            // exited are settled by cleanUpStagingDirs() once they expire.
            if (jobExecutor.ownsJob(job.jobExecutionId())) {
              pullAndUpdateOwnedJobStatus(metalake, job);
            }
          });
    }
  }

  private JobEntity toUpdatedStatusJobEntity(
      JobEntity latestJobEntity, JobHandle.Status observedStatus) {
    JobHandle.Status currentStatus = latestJobEntity.status();
    boolean observedIsFinished = isFinishedStatus(observedStatus);

    // Never regress a job out of a terminal state, and never move a CANCELLING job back to a
    // non-terminal state - both would only be possible here because the executor status was
    // observed against a stale snapshot of the job.
    if (isFinishedStatus(currentStatus)
        || (currentStatus == JobHandle.Status.CANCELLING && !observedIsFinished)) {
      return latestJobEntity;
    }

    // Only a directly-observed STARTED transition is trustworthy evidence of when a job started.
    // SUCCEEDED/FAILED do not prove the job ever reached STARTED: FAILED in particular can be
    // reached directly from QUEUED (e.g. NoSuchJobException from the executor, or
    // LocalJobExecutor failing before it records STARTED), and even for SUCCEEDED, backfilling
    // startedAt from the queued time would understate queue latency and overstate execution
    // duration in any derived metric. So startedAt is left unset unless a STARTED transition was
    // actually observed.
    //
    // Only stamp startedAt on the first STARTED observation (latestJobEntity.startedAt() <= 0).
    // A CANCELLING job already carries forward a real startedAt from cancelJob, and since
    // cancellation is asynchronous, a poll can still observe STARTED while cancellation is in
    // flight - overwriting the recorded start time with this later poll timestamp would lose the
    // accurate value.
    boolean isStarted = observedStatus == JobHandle.Status.STARTED;
    long startedAt =
        isStarted && latestJobEntity.startedAt() <= 0
            ? Instant.now().toEpochMilli()
            : latestJobEntity.startedAt();

    // Preserve an already-recorded finishedAt (e.g. stamped by a concurrent writer) instead of
    // overwriting it with a later poll's timestamp.
    long finishedAt =
        observedIsFinished
            ? (latestJobEntity.finishedAt() > 0
                ? latestJobEntity.finishedAt()
                : Instant.now().toEpochMilli())
            : latestJobEntity.finishedAt();

    return JobEntity.builder()
        .withId(latestJobEntity.id())
        .withJobExecutionId(latestJobEntity.jobExecutionId())
        .withJobTemplateName(latestJobEntity.jobTemplateName())
        .withStatus(observedStatus)
        .withNamespace(latestJobEntity.namespace())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(latestJobEntity.auditInfo().creator())
                .withCreateTime(latestJobEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .withStartedAt(startedAt)
        .withFinishedAt(finishedAt)
        // The runtime job template is fixed at job creation and never changes.
        .withRuntimeJobTemplate(latestJobEntity.runtimeJobTemplate())
        .build();
  }

  private static boolean isFinishedStatus(JobHandle.Status status) {
    return status == JobHandle.Status.SUCCEEDED
        || status == JobHandle.Status.FAILED
        || status == JobHandle.Status.CANCELLED;
  }

  @VisibleForTesting
  void cleanUpStagingDirs() {
    List<String> metalakes = MetalakeManager.listInUseMetalakes(entityStore);

    for (String metalake : metalakes) {
      long now = System.currentTimeMillis();
      List<JobEntity> expiredJobs = new ArrayList<>();
      for (JobEntity job : listJobs(metalake, Optional.empty())) {
        if (isFinishedStatus(job.status())) {
          if (job.finishedAt() > 0 && job.finishedAt() + jobStagingDirKeepTimeInMs < now) {
            expiredJobs.add(job);
          }
        } else if (jobExecutor.isJobStateNodeLocal() && isStaleActiveJob(job, now)) {
          // The state of a node local job is lost when the Gravitino server running it exits, so
          // an active job that has not been updated for the whole retention time is considered
          // left behind. Mark it as finished, and it is cleaned up once it expires as a finished
          // job. Jobs of other job executors can be tracked by any server, so they never expire.
          try {
            expireStaleActiveJob(metalake, job, now);
          } catch (RuntimeException e) {
            LOG.error("Failed to expire job {} under metalake {}", job.name(), metalake, e);
          }
        }
      }

      expiredJobs.forEach(
          job -> {
            try {
              entityStore.delete(
                  NameIdentifierUtil.ofJob(metalake, job.name()), Entity.EntityType.JOB);

              String jobStagingPath =
                  stagingDir.getAbsolutePath()
                      + String.format(JOB_STAGING_DIR, metalake, job.jobTemplateName(), job.id());
              File jobStagingDir = new File(jobStagingPath);
              if (jobStagingDir.exists()) {
                FileUtils.deleteDirectory(jobStagingDir);
                LOG.info("Deleted job staging directory {} for job {}", jobStagingPath, job.name());
              }
            } catch (OptimisticLockException e) {
              // Keep the files when deletion loses its CAS. The next cleanup run re-reads the
              // job and checks retention eligibility again; this batch can process other jobs.
              LOG.info(
                  "Job {} under metalake {} changed concurrently; deferring cleanup",
                  job.name(),
                  metalake);
            } catch (IOException e) {
              LOG.error("Failed to delete job and staging directory for job {}", job.name(), e);
            }
          });
    }
  }

  @VisibleForTesting
  public static JobTemplate createRuntimeJobTemplate(
      JobTemplateEntity jobTemplateEntity, Map<String, String> jobConf, File stagingDir) {
    String name = jobTemplateEntity.name();
    String comment = jobTemplateEntity.comment();

    JobTemplateEntity.TemplateContent content = jobTemplateEntity.templateContent();
    String executable =
        fetchFileFromUri(
            replacePlaceholder(content.executable(), jobConf), stagingDir, TIMEOUT_IN_MS);

    List<String> args =
        content.arguments().stream()
            .map(arg -> replacePlaceholder(arg, jobConf))
            .collect(Collectors.toList());
    Map<String, String> environments =
        content.environments().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> replacePlaceholder(entry.getKey(), jobConf),
                    entry -> replacePlaceholder(entry.getValue(), jobConf)));
    Map<String, String> customFields =
        content.customFields().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> replacePlaceholder(entry.getKey(), jobConf),
                    entry -> replacePlaceholder(entry.getValue(), jobConf)));

    // For shell job template
    if (content.jobType() == JobTemplate.JobType.SHELL) {
      List<String> scripts =
          content.scripts().stream()
              .map(
                  script ->
                      fetchFileFromUri(
                          replacePlaceholder(script, jobConf), stagingDir, TIMEOUT_IN_MS))
              .collect(Collectors.toList());

      return ShellJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withScripts(scripts)
          .build();
    }

    // For Spark job template
    if (content.jobType() == JobTemplate.JobType.SPARK) {
      String className = replacePlaceholder(content.className(), jobConf);
      List<String> jars =
          content.jars().stream()
              .map(
                  jar ->
                      fetchFileFromUri(replacePlaceholder(jar, jobConf), stagingDir, TIMEOUT_IN_MS))
              .collect(Collectors.toList());

      List<String> files =
          content.files().stream()
              .map(
                  file ->
                      fetchFileFromUri(
                          replacePlaceholder(file, jobConf), stagingDir, TIMEOUT_IN_MS))
              .collect(Collectors.toList());

      List<String> archives =
          content.archives().stream()
              .map(
                  archive ->
                      fetchFileFromUri(
                          replacePlaceholder(archive, jobConf), stagingDir, TIMEOUT_IN_MS))
              .collect(Collectors.toList());

      Map<String, String> configs =
          content.configs().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> replacePlaceholder(entry.getKey(), jobConf),
                      entry -> replacePlaceholder(entry.getValue(), jobConf)));

      return SparkJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withClassName(className)
          .withJars(jars)
          .withFiles(files)
          .withArchives(archives)
          .withConfigs(configs)
          .build();
    }

    throw new IllegalArgumentException("Unsupported job type: " + content.jobType());
  }

  @VisibleForTesting
  static String replacePlaceholder(String inputString, Map<String, String> replacements) {
    if (StringUtils.isBlank(inputString)) {
      return inputString; // Return as is if the input string is blank
    }

    StringBuilder result = new StringBuilder();

    Matcher matcher = PLACEHOLDER_PATTERN.matcher(inputString);
    while (matcher.find()) {
      String key = matcher.group(1);
      String replacement = replacements.get(key);
      if (replacement != null) {
        matcher.appendReplacement(result, replacement);
      } else {
        // If no replacement is found, keep the placeholder as is
        matcher.appendReplacement(result, matcher.group(0));
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }

  @VisibleForTesting
  static List<String> fetchFilesFromUri(List<String> uris, File stagingDir, int timeoutInMs) {
    return uris.stream()
        .map(uri -> fetchFileFromUri(uri, stagingDir, timeoutInMs))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static String fetchFileFromUri(String uri, File stagingDir, int timeoutInMs) {
    try {
      URI fileUri = new URI(uri);
      File destFile = new File(stagingDir, new File(fileUri.getPath()).getName());
      return FileFetcher.get()
          .fetchFileFromUri(
              uri,
              destFile,
              timeoutInMs,
              null /* hadoopConf: job file URIs never use the hdfs scheme */);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch file from URI %s", uri), e);
    }
  }

  @VisibleForTesting
  JobTemplateEntity updateJobTemplateEntity(
      NameIdentifier jobTemplateIdent,
      JobTemplateEntity jobTemplateEntity,
      JobTemplateChange... changes) {
    String newName = jobTemplateEntity.name();
    String newComment = jobTemplateEntity.comment();
    JobTemplateEntity.Builder newTemplateBuilder = JobTemplateEntity.builder();
    JobTemplateEntity.TemplateContent.TemplateContentBuilder newTemplateContentBuilder =
        JobTemplateEntity.TemplateContent.builder()
            .withJobType(jobTemplateEntity.templateContent().jobType())
            .withExecutable(jobTemplateEntity.templateContent().executable())
            .withArguments(jobTemplateEntity.templateContent().arguments())
            .withEnvironments(jobTemplateEntity.templateContent().environments())
            .withCustomFields(jobTemplateEntity.templateContent().customFields())
            .withScripts(jobTemplateEntity.templateContent().scripts())
            .withClassName(jobTemplateEntity.templateContent().className())
            .withJars(jobTemplateEntity.templateContent().jars())
            .withFiles(jobTemplateEntity.templateContent().files())
            .withArchives(jobTemplateEntity.templateContent().archives())
            .withConfigs(jobTemplateEntity.templateContent().configs());

    for (JobTemplateChange change : changes) {
      if (change instanceof JobTemplateChange.RenameJobTemplate) {
        newName = ((JobTemplateChange.RenameJobTemplate) change).getNewName();

      } else if (change instanceof JobTemplateChange.UpdateJobTemplateComment) {
        newComment = ((JobTemplateChange.UpdateJobTemplateComment) change).getNewComment();

      } else if (change instanceof JobTemplateChange.UpdateJobTemplate) {
        JobTemplateEntity.TemplateContent oldTemplateContent = jobTemplateEntity.templateContent();
        JobTemplateChange.TemplateUpdate templateUpdate =
            ((JobTemplateChange.UpdateJobTemplate) change).getTemplateUpdate();
        newTemplateContentBuilder
            .withJobType(oldTemplateContent.jobType())
            .withExecutable(
                updatedValue(
                    oldTemplateContent.executable(),
                    Optional.ofNullable(templateUpdate.getNewExecutable())))
            .withArguments(
                updatedValue(
                    oldTemplateContent.arguments(),
                    Optional.ofNullable(templateUpdate.getNewArguments())))
            .withEnvironments(
                updatedValue(
                    oldTemplateContent.environments(),
                    Optional.ofNullable(templateUpdate.getNewEnvironments())))
            .withCustomFields(
                updatedValue(
                    oldTemplateContent.customFields(),
                    Optional.ofNullable(templateUpdate.getNewCustomFields())));

        if (templateUpdate instanceof JobTemplateChange.ShellTemplateUpdate) {
          Preconditions.checkArgument(
              jobTemplateEntity.templateContent().jobType() == JobTemplate.JobType.SHELL,
              "Job template %s is not a shell job template, cannot update to shell template",
              jobTemplateIdent.name());

          JobTemplateChange.ShellTemplateUpdate shellUpdate =
              (JobTemplateChange.ShellTemplateUpdate) templateUpdate;
          newTemplateContentBuilder.withScripts(
              updatedValue(
                  oldTemplateContent.scripts(), Optional.ofNullable(shellUpdate.getNewScripts())));

        } else if (templateUpdate instanceof JobTemplateChange.SparkTemplateUpdate) {
          Preconditions.checkArgument(
              jobTemplateEntity.templateContent().jobType() == JobTemplate.JobType.SPARK,
              "Job template %s is not a spark job template, cannot update to spark template",
              jobTemplateIdent.name());

          JobTemplateChange.SparkTemplateUpdate sparkUpdate =
              (JobTemplateChange.SparkTemplateUpdate) templateUpdate;
          newTemplateContentBuilder
              .withClassName(
                  updatedValue(
                      oldTemplateContent.className(),
                      Optional.ofNullable(sparkUpdate.getNewClassName())))
              .withJars(
                  updatedValue(
                      oldTemplateContent.jars(), Optional.ofNullable(sparkUpdate.getNewJars())))
              .withFiles(
                  updatedValue(
                      oldTemplateContent.files(), Optional.ofNullable(sparkUpdate.getNewFiles())))
              .withArchives(
                  updatedValue(
                      oldTemplateContent.archives(),
                      Optional.ofNullable(sparkUpdate.getNewArchives())))
              .withConfigs(
                  updatedValue(
                      oldTemplateContent.configs(),
                      Optional.ofNullable(sparkUpdate.getNewConfigs())));

        } else {
          throw new IllegalArgumentException("Unsupported template update: " + templateUpdate);
        }

      } else {
        throw new IllegalArgumentException("Unsupported job template change: " + change);
      }
    }

    return newTemplateBuilder
        .withId(jobTemplateEntity.id())
        .withName(newName)
        .withComment(newComment)
        .withNamespace(jobTemplateIdent.namespace())
        .withTemplateContent(newTemplateContentBuilder.build())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(jobTemplateEntity.auditInfo().creator())
                .withCreateTime(jobTemplateEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private void deleteStagingDirOfUnsubmittedJob(File jobStagingDir, long jobId) {
    // The job is not tracked by any job entity, so the periodic cleanup will never remove its
    // staging directory. A cleanup failure must not mask the original submission failure.
    try {
      FileUtils.deleteDirectory(jobStagingDir);
    } catch (IOException e) {
      LOG.warn(
          "Failed to delete staging directory {} of job {} whose submission failed",
          jobStagingDir,
          jobId,
          e);
    }
  }

  private <T> T updatedValue(T currentValue, Optional<T> newValue) {
    return newValue.orElse(currentValue);
  }

  private void pullAndUpdateOwnedJobStatus(String metalake, JobEntity job) {
    JobHandle.Status newStatus = job.status();
    try {
      newStatus = jobExecutor.getJobStatus(job.jobExecutionId());
      // The job was marked as CANCELLING by another server, which can't cancel it itself, so
      // cancel it here as the owner. This only applies to node local job state, other job
      // executors are cancelled directly by the server handling the request, and they may keep
      // reporting the job as running while cancelling it asynchronously.
      if (jobExecutor.isJobStateNodeLocal()
          && job.status() == JobHandle.Status.CANCELLING
          && (newStatus == JobHandle.Status.QUEUED || newStatus == JobHandle.Status.STARTED)) {
        newStatus = cancelOwnedJob(metalake, job);
      }
    } catch (NoSuchJobException e) {
      // If the job is not found in the external job executor, we assume the job is
      // FAILED if it is not in CANCELLING status, otherwise we assume it is CANCELLED.
      if (job.status() == JobHandle.Status.CANCELLING) {
        newStatus = JobHandle.Status.CANCELLED;
      } else {
        newStatus = JobHandle.Status.FAILED;
      }
      LOG.warn(
          "Job {} with execution id {} under metalake {} is not found in the "
              + "external job executor, marking it as {}. This could be due to the job "
              + "being deleted by the external job executor. Please check the external job "
              + "executor to know more details.",
          job.name(),
          job.jobExecutionId(),
          metalake,
          newStatus);
    } catch (Exception e) {
      // Keep the job unchanged, and retry it in the next poll.
      newStatus = job.status();
      LOG.error(
          "Failed to pull or cancel job {} by execution id {}",
          job.name(),
          job.jobExecutionId(),
          e);
    }

    if (newStatus != job.status()) {
      // Update the job entity with new status. entityStore.update() re-fetches the
      // latest entity itself right before applying the updater, so the transition below
      // is derived from latestJobEntity - the state as of right before the write - rather
      // than the possibly-stale `job` snapshot taken by listJobs() above. A concurrent
      // writer (e.g. cancelJob(), or another poll run) may have already moved the job to
      // a terminal state, into CANCELLING, or recorded a real startedAt/finishedAt in the
      // gap between that snapshot and this point; the updater must not regress any of
      // that using the stale snapshot's view of the world.
      JobHandle.Status finalNewStatus = newStatus;
      updateJobEntity(
              metalake,
              job,
              latestJobEntity -> toUpdatedStatusJobEntity(latestJobEntity, finalNewStatus))
          .ifPresent(
              updated ->
                  LOG.info(
                      "Updated the job {} with execution id {} status to {}",
                      job.name(),
                      job.jobExecutionId(),
                      updated.status()));
    }
  }

  private JobHandle.Status cancelOwnedJob(String metalake, JobEntity job) {
    LOG.info(
        "Cancelling job {} with execution id {} under metalake {} as it is marked as CANCELLING",
        job.name(),
        job.jobExecutionId(),
        metalake);
    jobExecutor.cancelJob(job.jobExecutionId());
    return jobExecutor.getJobStatus(job.jobExecutionId());
  }

  private Optional<JobEntity> updateJobEntity(
      String metalake, JobEntity job, Function<JobEntity, JobEntity> updater) {
    try {
      return Optional.of(
          TreeLockUtils.doWithTreeLock(
              NameIdentifierUtil.ofJob(metalake, job.name()),
              LockType.WRITE,
              () -> {
                try {
                  return entityStore.update(
                      NameIdentifierUtil.ofJob(metalake, job.name()),
                      JobEntity.class,
                      Entity.EntityType.JOB,
                      updater);
                } catch (IOException e) {
                  throw new RuntimeException(
                      String.format("Failed to update job entity %s", job.name()), e);
                }
              }));
    } catch (OptimisticLockException e) {
      // A later poll re-reads both executor state and metadata. Never stop the scheduled
      // task or replay external submission/cancellation because a metadata CAS lost.
      LOG.info(
          "Job {} under metalake {} changed concurrently; deferring status update",
          job.name(),
          metalake);
      return Optional.empty();
    } catch (NoSuchEntityException e) {
      // The job could have been deleted concurrently (e.g. by legacy-timeline cleanup)
      // in the gap between the listJobs() snapshot above and this update. Skip it rather
      // than letting the exception escape this scheduled task, which would silently
      // cancel all future status-pull runs (ScheduledExecutorService semantics).
      LOG.warn(
          "Job {} under metalake {} no longer exists, skipping the update. This could be due "
              + "to the job being deleted concurrently.",
          job.name(),
          metalake);
      return Optional.empty();
    }
  }

  private void expireStaleActiveJob(String metalake, JobEntity job, long now) {
    AtomicBoolean expired = new AtomicBoolean(false);
    updateJobEntity(
            metalake,
            job,
            latestJobEntity -> {
              // The job may have been updated since the listJobs() snapshot.
              if (!isStaleActiveJob(latestJobEntity, now)) {
                return latestJobEntity;
              }
              expired.set(true);
              // The expired job gets the current time as its finished time, so it's kept for
              // another retention time like any other finished job before being cleaned up.
              return toUpdatedStatusJobEntity(
                  latestJobEntity,
                  latestJobEntity.status() == JobHandle.Status.CANCELLING
                      ? JobHandle.Status.CANCELLED
                      : JobHandle.Status.FAILED);
            })
        .filter(expiredJob -> expired.get())
        .ifPresent(
            expiredJob ->
                LOG.warn(
                    "Job {} with execution id {} under metalake {} has not been updated for more "
                        + "than {} ms, marking it as {}. This could be due to the Gravitino server "
                        + "running the job having exited.",
                    job.name(),
                    job.jobExecutionId(),
                    metalake,
                    jobStagingDirKeepTimeInMs,
                    expiredJob.status()));
  }

  private boolean isStaleActiveJob(JobEntity job, long now) {
    return !isFinishedStatus(job.status())
        && lastUpdatedTimeInMs(job) + jobStagingDirKeepTimeInMs < now;
  }

  private static long lastUpdatedTimeInMs(JobEntity job) {
    AuditInfo auditInfo = job.auditInfo();
    Instant lastUpdatedTime =
        auditInfo.lastModifiedTime() != null
            ? auditInfo.lastModifiedTime()
            : auditInfo.createTime();
    return lastUpdatedTime == null ? 0L : lastUpdatedTime.toEpochMilli();
  }
}
