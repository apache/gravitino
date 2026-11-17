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
package org.apache.gravitino.maintenance;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Submits TMS Spark jobs and binds the resulting job run id onto soft-slot claims.
 *
 * <p>{@link #submitAndBind} injects {@code action_ids}, calls {@code runJob}, then binds {@code
 * job_id}. When authorization is enabled, {@code runJob} runs as the configured {@link
 * TableMaintenanceConfig#JOB_SUBMITTER} USER so {@link org.apache.gravitino.job.JobManager} sets a
 * stable job owner (including for GROUP-owned metalakes). If {@code runJob} succeeds but bind
 * fails, the orphaned Spark job is cancelled and unbound claims are released. Callers that fail
 * before {@code runJob} must release claims themselves.
 */
public class MaintenanceJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceJobManager.class);

  private static final TableMaintenanceActionMetaService ACTION_SERVICE =
      TableMaintenanceActionMetaService.getInstance();

  private static volatile MaintenanceJobManager instance;

  private final JobOperationDispatcher jobOperationDispatcher;
  private final boolean authorizationEnabled;
  @Nullable private final String jobSubmitter;

  /**
   * Returns the shared TMS job manager for the server process.
   *
   * <p>Composition roots (for example the table-maintenance REST feature) resolve dispatchers from
   * {@link org.apache.gravitino.GravitinoEnv} and pass them in.
   *
   * @param jobOperationDispatcher job submit/cancel dispatcher
   * @param authorizationEnabled whether {@code gravitino.authorization.enable} is true
   * @param jobSubmitter configured submitter USER when authorization is enabled, else {@code null}
   * @return singleton manager
   */
  public static MaintenanceJobManager getInstance(
      JobOperationDispatcher jobOperationDispatcher,
      boolean authorizationEnabled,
      @Nullable String jobSubmitter) {
    if (instance == null) {
      synchronized (MaintenanceJobManager.class) {
        if (instance == null) {
          instance =
              new MaintenanceJobManager(
                  jobOperationDispatcher, authorizationEnabled, jobSubmitter);
        }
      }
    }
    return instance;
  }

  /**
   * Test / override constructor. Production code should use {@link #getInstance}.
   *
   * @param jobOperationDispatcher job submit/cancel dispatcher
   * @param authorizationEnabled whether authorization is enabled
   * @param jobSubmitter configured submitter USER when authorization is enabled, else {@code null}
   */
  MaintenanceJobManager(
      JobOperationDispatcher jobOperationDispatcher,
      boolean authorizationEnabled,
      @Nullable String jobSubmitter) {
    this.jobOperationDispatcher = Preconditions.checkNotNull(jobOperationDispatcher);
    this.authorizationEnabled = authorizationEnabled;
    if (authorizationEnabled) {
      Preconditions.checkArgument(
          jobSubmitter != null && !jobSubmitter.trim().isEmpty(),
          "jobSubmitter is required when authorization is enabled");
      this.jobSubmitter = jobSubmitter.trim();
    } else {
      this.jobSubmitter = null;
    }
  }

  /**
   * Injects {@code action_ids}, runs the TMS maintenance template, and binds {@code job_id} on the
   * claims (one claim for cron, several for a commit chain).
   *
   * @param metalake metalake name
   * @param jobConf mutable runtime jobConf (policy options already resolved)
   * @param claims soft-slot claims from acquire (non-empty)
   * @return job run id when run+bind succeeded; empty when submit or bind failed (claims released)
   */
  public Optional<Long> submitAndBind(
      String metalake, Map<String, String> jobConf, List<OccupancyClaim> claims) {
    Preconditions.checkNotNull(metalake, "metalake");
    Preconditions.checkNotNull(jobConf, "jobConf");
    Preconditions.checkArgument(claims != null && !claims.isEmpty(), "claims must be non-empty");

    jobConf.put(
        MaintenanceJobTemplates.ACTION_IDS_KEY,
        claims.stream().map(c -> Long.toString(c.actionId())).collect(Collectors.joining(",")));

    Long jobId = null;
    try {
      JobEntity job = runIcebergMaintenanceJob(metalake, jobConf);
      jobId = job.id();
      ACTION_SERVICE.bindJobIds(claims, jobId);
      return Optional.of(jobId);
    } catch (RuntimeException e) {
      if (jobId != null) {
        try {
          jobOperationDispatcher.cancelJob(metalake, JobHandle.JOB_ID_PREFIX + jobId);
        } catch (RuntimeException cancelError) {
          LOG.warn(
              "Failed to cancel orphaned TMS Spark job {} under metalake {} after bind failure",
              jobId,
              metalake,
              cancelError);
        }
      }
      try {
        ACTION_SERVICE.deleteClaims(claims);
      } catch (RuntimeException releaseError) {
        LOG.warn("Failed to release occupancy claims after submit failure", releaseError);
      }
      LOG.warn(
          "Failed TMS job submitAndBind for action_ids {}: {}",
          claims.stream().map(c -> Long.toString(c.actionId())).collect(Collectors.joining(",")),
          e.toString());
      return Optional.empty();
    }
  }

  /**
   * Runs the Iceberg maintenance template.
   *
   * <p>When authorization is disabled, calls {@code runJob} on the current thread. When
   * authorization is enabled, runs the same {@code runJob} under {@code doAs(jobSubmitter)} so
   * {@link org.apache.gravitino.job.JobManager} records that USER as the job owner.
   */
  JobEntity runIcebergMaintenanceJob(String metalake, Map<String, String> jobConf) {
    if (!authorizationEnabled) {
      return runJob(metalake, jobConf);
    }
    try {
      return PrincipalUtils.doAs(new UserPrincipal(jobSubmitter), () -> runJob(metalake, jobConf));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit TMS job as submitter " + jobSubmitter, e);
    }
  }

  private JobEntity runJob(String metalake, Map<String, String> jobConf) {
    return jobOperationDispatcher.runJob(
        metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
  }
}
