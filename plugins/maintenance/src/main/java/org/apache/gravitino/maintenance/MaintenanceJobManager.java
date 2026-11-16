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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
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
 * job_id}. Cron and commit submit run on scheduler threads without an authenticated subject, so
 * when ownership is enabled this path runs {@code runJob} as the metalake owner user. Otherwise
 * {@code JobManager} would set the job owner to {@code anonymous}, which often does not exist and
 * fails submit. If {@code runJob} succeeds but bind fails, the orphaned Spark job is cancelled and
 * unbound claims are released. Callers that fail before {@code runJob} must release claims
 * themselves.
 */
public class MaintenanceJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceJobManager.class);

  private static final TableMaintenanceActionMetaService ACTION_SERVICE =
      TableMaintenanceActionMetaService.getInstance();

  private static volatile MaintenanceJobManager instance;

  private final JobOperationDispatcher jobOperationDispatcher;

  /**
   * Returns the shared TMS job manager for the server process.
   *
   * @return singleton manager
   */
  public static MaintenanceJobManager getInstance() {
    if (instance == null) {
      synchronized (MaintenanceJobManager.class) {
        if (instance == null) {
          instance = new MaintenanceJobManager(GravitinoEnv.getInstance().jobOperationDispatcher());
        }
      }
    }
    return instance;
  }

  private MaintenanceJobManager(JobOperationDispatcher jobOperationDispatcher) {
    this.jobOperationDispatcher = Preconditions.checkNotNull(jobOperationDispatcher);
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
      JobEntity job = runJobAsMetalakeOwner(metalake, jobConf);
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

  private JobEntity runJobAsMetalakeOwner(String metalake, Map<String, String> jobConf) {
    OwnerDispatcher owners = GravitinoEnv.getInstance().internalOwnerDispatcher();
    // Ownership disabled: JobManager skips setOwner; no principal switch needed.
    if (owners == null) {
      return jobOperationDispatcher.runJob(
          metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
    }
    String ownerUser = requireMetalakeOwnerUser(owners, metalake);
    try {
      return PrincipalUtils.doAs(
          new UserPrincipal(ownerUser),
          () ->
              jobOperationDispatcher.runJob(
                  metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to submit TMS job as metalake owner " + ownerUser, e);
    }
  }

  /**
   * Resolves the metalake owner user used as the {@code runJob} principal when ownership is
   * enabled. Job ownership assignment requires a USER entity; group owners are rejected.
   *
   * @param owners owner dispatcher (non-null)
   * @param metalake metalake name
   * @return metalake owner user name
   */
  @VisibleForTesting
  static String requireMetalakeOwnerUser(OwnerDispatcher owners, String metalake) {
    Preconditions.checkNotNull(owners, "owners");
    Preconditions.checkNotNull(metalake, "metalake");
    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE);
    Optional<Owner> owner = owners.getOwner(metalake, metalakeObject);
    if (owner.isEmpty()) {
      throw new IllegalStateException("Metalake " + metalake + " has no owner for TMS job submit");
    }
    if (owner.get().type() != Owner.Type.USER) {
      throw new IllegalStateException(
          "Metalake "
              + metalake
              + " owner must be a USER for TMS job submit, got "
              + owner.get().type()
              + " "
              + owner.get().name());
    }
    String name = owner.get().name();
    if (name == null || name.trim().isEmpty()) {
      throw new IllegalStateException("Metalake " + metalake + " owner user name is blank");
    }
    return name;
  }
}
