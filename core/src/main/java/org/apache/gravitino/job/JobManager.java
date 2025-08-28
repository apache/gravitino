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

import static org.apache.gravitino.Metalake.PROPERTY_IN_USE;
import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.IdGenerator;
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

  private final ScheduledExecutorService cleanUpExecutor;

  private final ScheduledExecutorService statusPullExecutor;

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
      if (!stagingDir.mkdirs()) {
        throw new IllegalArgumentException(
            String.format("Failed to create staging directory %s", stagingDirPath));
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

    boolean hasActiveJobs =
        jobs.stream()
            .anyMatch(
                job ->
                    job.status() != JobHandle.Status.CANCELLED
                        && job.status() != JobHandle.Status.SUCCEEDED
                        && job.status() != JobHandle.Status.FAILED);
    if (hasActiveJobs) {
      throw new InUseException(
          "Job template %s under metalake %s has active jobs associated with it",
          jobTemplateName, metalake);
    }

    // Delete all the job staging directories associated with the job template.
    String jobTemplateStagingPath =
        stagingDir.getAbsolutePath() + File.separator + metalake + File.separator + jobTemplateName;
    File jobTemplateStagingDir = new File(jobTemplateStagingPath);
    if (jobTemplateStagingDir.exists()) {
      try {
        FileUtils.deleteDirectory(jobTemplateStagingDir);
      } catch (IOException e) {
        LOG.error("Failed to delete job template staging directory: {}", jobTemplateStagingPath, e);
      }
    }

    // Delete the job template entity as well as all the jobs associated with it.
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofJobTemplate(metalake).levels()),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.delete(
                NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName),
                Entity.EntityType.JOB_TEMPLATE);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
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
  public JobEntity getJob(String metalake, String jobId) throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobIdent = NameIdentifierUtil.ofJob(metalake, jobId);
    return TreeLockUtils.doWithTreeLock(
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
    if (!jobStagingDir.mkdirs()) {
      throw new RuntimeException(
          String.format("Failed to create staging directory %s for job %s", jobStagingDir, jobId));
    }

    // Create a JobTemplate by replacing the template parameters with the jobConf values, and
    // also downloading any necessary files from the URIs specified in the job template.
    JobTemplate jobTemplate = createRuntimeJobTemplate(jobTemplateEntity, jobConf, jobStagingDir);

    // Submit the job template to the job executor
    String jobExecutionId;
    try {
      jobExecutionId = jobExecutor.submitJob(jobTemplate);
    } catch (Exception e) {
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
            .build();

    try {
      entityStore.put(jobEntity, false /* overwrite */);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register the job entity " + jobEntity, e);
    }

    return jobEntity;
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    // Retrieve the job entity, will throw NoSuchJobException if the job does not exist.
    JobEntity jobEntity = getJob(metalake, jobId);

    if (jobEntity.status() == JobHandle.Status.CANCELLING
        || jobEntity.status() == JobHandle.Status.CANCELLED
        || jobEntity.status() == JobHandle.Status.SUCCEEDED
        || jobEntity.status() == JobHandle.Status.FAILED) {
      // If the job is already cancelling, cancelled, succeeded, or failed, we do not need to cancel
      // it again.
      return jobEntity;
    }

    // Cancel the job using the job executor
    try {
      jobExecutor.cancelJob(jobEntity.jobExecutionId());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to cancel job with ID %s under metalake %s", jobId, metalake), e);
    }

    // Update the job status to CANCELING
    JobEntity newJobEntity =
        JobEntity.builder()
            .withId(jobEntity.id())
            .withJobExecutionId(jobEntity.jobExecutionId())
            .withJobTemplateName(jobEntity.jobTemplateName())
            .withStatus(JobHandle.Status.CANCELLING)
            .withNamespace(jobEntity.namespace())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(jobEntity.auditInfo().creator())
                    .withCreateTime(jobEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofJob(metalake, jobId),
        LockType.WRITE,
        () -> {
          try {
            // Update the job entity in the entity store
            entityStore.put(newJobEntity, true /* overwrite */);
            return newJobEntity;
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Failed to update job entity %s to CANCELING status", newJobEntity),
                e);
          }
        });
  }

  @Override
  public void close() throws IOException {
    jobExecutor.close();
    statusPullExecutor.shutdownNow();
    cleanUpExecutor.shutdownNow();
  }

  @VisibleForTesting
  void pullAndUpdateJobStatus() {
    List<String> metalakes = listInUseMetalakes(entityStore);
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
            JobHandle.Status newStatus = jobExecutor.getJobStatus(job.jobExecutionId());
            if (newStatus != job.status()) {
              JobEntity newJobEntity =
                  JobEntity.builder()
                      .withId(job.id())
                      .withJobExecutionId(job.jobExecutionId())
                      .withJobTemplateName(job.jobTemplateName())
                      .withStatus(newStatus)
                      .withNamespace(job.namespace())
                      .withAuditInfo(
                          AuditInfo.builder()
                              .withCreator(job.auditInfo().creator())
                              .withCreateTime(job.auditInfo().createTime())
                              .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                              .withLastModifiedTime(Instant.now())
                              .build())
                      .build();

              // Update the job entity with new status.
              TreeLockUtils.doWithTreeLock(
                  NameIdentifierUtil.ofJob(metalake, job.name()),
                  LockType.WRITE,
                  () -> {
                    try {
                      entityStore.put(newJobEntity, true /* overwrite */);
                      return null;
                    } catch (IOException e) {
                      throw new RuntimeException(
                          String.format(
                              "Failed to update job entity %s to status %s",
                              newJobEntity, newStatus),
                          e);
                    }
                  });

              LOG.info(
                  "Updated the job {} with execution id {} status to {}",
                  job.name(),
                  job.jobExecutionId(),
                  newStatus);
            }
          });
    }
  }

  @VisibleForTesting
  void cleanUpStagingDirs() {
    List<String> metalakes = listInUseMetalakes(entityStore);

    for (String metalake : metalakes) {
      List<JobEntity> finishedJobs =
          listJobs(metalake, Optional.empty()).stream()
              .filter(
                  job ->
                      job.status() == JobHandle.Status.CANCELLED
                          || job.status() == JobHandle.Status.SUCCEEDED
                          || job.status() == JobHandle.Status.FAILED)
              .filter(
                  job ->
                      job.finishedAt() > 0
                          && job.finishedAt() + jobStagingDirKeepTimeInMs
                              < System.currentTimeMillis())
              .toList();

      finishedJobs.forEach(
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
    String executable = fetchFileFromUri(content.executable(), stagingDir, TIMEOUT_IN_MS);

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
      List<String> scripts = fetchFilesFromUri(content.scripts(), stagingDir, TIMEOUT_IN_MS);

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
      String className = content.className();
      List<String> jars = fetchFilesFromUri(content.jars(), stagingDir, TIMEOUT_IN_MS);
      List<String> files = fetchFilesFromUri(content.files(), stagingDir, TIMEOUT_IN_MS);
      List<String> archives = fetchFilesFromUri(content.archives(), stagingDir, TIMEOUT_IN_MS);
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
      String scheme = Optional.ofNullable(fileUri.getScheme()).orElse("file");
      File destFile = new File(stagingDir, new File(fileUri.getPath()).getName());

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          FileUtils.copyURLToFile(fileUri.toURL(), destFile, timeoutInMs, timeoutInMs);
          break;

        case "file":
          Files.createSymbolicLink(destFile.toPath(), new File(fileUri.getPath()).toPath());
          break;

        default:
          throw new IllegalArgumentException("Unsupported scheme: " + scheme);
      }

      return destFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch file from URI %s", uri), e);
    }
  }

  private static List<String> listInUseMetalakes(EntityStore entityStore) {
    try {
      List<BaseMetalake> metalakes =
          TreeLockUtils.doWithRootTreeLock(
              LockType.READ,
              () ->
                  entityStore.list(
                      Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE));
      return metalakes.stream()
          .filter(
              m -> (boolean) m.propertiesMetadata().getOrDefault(m.properties(), PROPERTY_IN_USE))
          .map(BaseMetalake::name)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Failed to list in-use metalakes", e);
    }
  }
}
