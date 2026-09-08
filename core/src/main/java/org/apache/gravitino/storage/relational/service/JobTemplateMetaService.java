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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

public class JobTemplateMetaService {

  private static final JobTemplateMetaService INSTANCE = new JobTemplateMetaService();

  private JobTemplateMetaService() {
    // Private constructor to prevent instantiation
  }

  public static JobTemplateMetaService getInstance() {
    return INSTANCE;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listJobTemplatesByNamespace")
  public List<JobTemplateEntity> listJobTemplatesByNamespace(Namespace ns) {
    String metalakeName = ns.level(0);
    List<JobTemplatePO> jobTemplatePOs =
        SessionUtils.getWithoutCommit(
            JobTemplateMetaMapper.class,
            mapper -> mapper.listJobTemplatePOsByMetalake(metalakeName));

    return jobTemplatePOs.stream()
        .map(p -> JobTemplatePO.fromJobTemplatePO(p, ns))
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getJobTemplateByIdentifier")
  public JobTemplateEntity getJobTemplateByIdentifier(NameIdentifier jobTemplateIdent) {
    JobTemplatePO jobTemplatePO = getJobTemplatePO(jobTemplateIdent);
    return JobTemplatePO.fromJobTemplatePO(jobTemplatePO, jobTemplateIdent.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertJobTemplate")
  public void insertJobTemplate(JobTemplateEntity jobTemplateEntity, boolean overwrite)
      throws IOException {
    String metalakeName = jobTemplateEntity.namespace().level(0);

    try {
      Long metalakeId =
          EntityIdService.getEntityId(NameIdentifier.of(metalakeName), Entity.EntityType.METALAKE);
      JobTemplatePO.JobTemplatePOBuilder builder =
          JobTemplatePO.builder().withMetalakeId(metalakeId);
      JobTemplatePO jobTemplatePO =
          JobTemplatePO.initializeJobTemplatePO(jobTemplateEntity, builder);

      SessionUtils.doMultipleWithCommit(
          () -> lockMetalake(metalakeName, metalakeId),
          () ->
              SessionUtils.doWithoutCommit(
                  JobTemplateMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertJobTemplateMetaOnDuplicateKeyUpdate(jobTemplatePO);
                    } else {
                      mapper.insertJobTemplateMeta(jobTemplatePO);
                    }
                  }));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.JOB_TEMPLATE, jobTemplateEntity.name());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteJobTemplate")
  public boolean deleteJobTemplate(NameIdentifier jobTemplateIdent) {
    try {
      deleteJobTemplateWithVersion(jobTemplateIdent, getJobTemplatePO(jobTemplateIdent));
      return true;
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteJobTemplatesByLegacyTimeline")
  public int deleteJobTemplatesByLegacyTimeline(long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        JobTemplateMetaMapper.class,
        mapper -> mapper.deleteJobTemplateMetasByLegacyTimeline(legacyTimeline, limit));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateJobTemplate")
  public <E extends Entity & HasIdentifier> JobTemplateEntity updateJobTemplate(
      NameIdentifier jobTemplateIdent, Function<E, E> updater) throws IOException {
    JobTemplatePO oldJobTemplatePO = getJobTemplatePO(jobTemplateIdent);
    JobTemplateEntity oldJobTemplateEntity =
        JobTemplatePO.fromJobTemplatePO(oldJobTemplatePO, jobTemplateIdent.namespace());
    JobTemplateEntity newJobTemplateEntity =
        (JobTemplateEntity) updater.apply((E) oldJobTemplateEntity);
    Preconditions.checkArgument(
        Objects.equals(oldJobTemplateEntity.id(), newJobTemplateEntity.id()),
        "The updated job templated id: %s is not equal to the old one: %s, which is unexpected",
        newJobTemplateEntity.id(),
        oldJobTemplateEntity.id());

    JobTemplatePO.JobTemplatePOBuilder newBuilder =
        JobTemplatePO.builder().withMetalakeId(oldJobTemplatePO.metalakeId());
    JobTemplatePO newJobTemplatePO =
        JobTemplatePO.updateJobTemplatePO(oldJobTemplatePO, newJobTemplateEntity, newBuilder);

    try {
      SessionUtils.doMultipleWithCommit(
          () ->
              OccWriteSupport.updateWithVersion(
                  () ->
                      SessionUtils.getWithoutCommit(
                          JobTemplateMetaMapper.class,
                          mapper ->
                              mapper.updateJobTemplateMeta(newJobTemplatePO, oldJobTemplatePO)),
                  () -> writeFailure(jobTemplateIdent, oldJobTemplatePO)));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.JOB_TEMPLATE, jobTemplateIdent.name());
      throw e;
    }
    return newJobTemplateEntity;
  }

  private JobTemplatePO getJobTemplatePO(NameIdentifier jobTemplateIdent) {
    String metalakeName = jobTemplateIdent.namespace().level(0);
    String jobTemplateName = jobTemplateIdent.name();

    JobTemplatePO jobTemplatePO =
        SessionUtils.getWithoutCommit(
            JobTemplateMetaMapper.class,
            mapper -> mapper.selectJobTemplatePOByMetalakeAndName(metalakeName, jobTemplateName));

    if (jobTemplatePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.JOB_TEMPLATE.name().toLowerCase(Locale.ROOT),
          jobTemplateName);
    }
    return jobTemplatePO;
  }

  public long getJobTemplateIdByMetalakeIdAndName(long metalakeId, String name) {
    Long jobTemplateId =
        SessionUtils.getWithoutCommit(
            JobTemplateMetaMapper.class,
            mapper -> mapper.selectJobTemplateIdByMetalakeAndName(metalakeId, name));

    if (jobTemplateId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.JOB_TEMPLATE.name().toLowerCase(Locale.ROOT),
          name);
    }
    return jobTemplateId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetJobTemplateByIdentifier")
  public List<JobTemplateEntity> batchGetJobTemplateByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    String metalakeName = firstIdent.namespace().level(0);
    List<String> jobTemplateNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        JobTemplateMetaMapper.class,
        mapper -> {
          List<JobTemplatePO> jobTemplatePOs =
              mapper.batchSelectJobTemplateByIdentifier(metalakeName, jobTemplateNames);
          return jobTemplatePOs.stream()
              .map(po -> JobTemplatePO.fromJobTemplatePO(po, firstIdent.namespace()))
              .collect(Collectors.toList());
        });
  }

  /** Deletes the observed template before its jobs, rolling back all changes on any failure. */
  void deleteJobTemplateWithVersion(NameIdentifier ident, JobTemplatePO observed) {
    SessionUtils.doMultipleWithCommit(
        () ->
            OccWriteSupport.deleteWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        JobTemplateMetaMapper.class,
                        mapper ->
                            mapper.softDeleteJobTemplateById(
                                observed.jobTemplateId(), observed.currentVersion())),
                () -> writeFailure(ident, observed)),
        () ->
            SessionUtils.doWithoutCommit(
                JobMetaMapper.class,
                mapper -> mapper.softDeleteJobsByTemplateId(observed.jobTemplateId())));
  }

  /** Locks the observed template while a job is inserted in the same transaction. */
  void lockTemplateForJobWrite(String name, Long templateId, Long metalakeId) {
    OccWriteSupport.lockParentForChildWrite(
        name,
        Entity.EntityType.JOB_TEMPLATE,
        () ->
            SessionUtils.getWithoutCommit(
                JobTemplateMetaMapper.class,
                mapper -> mapper.selectJobTemplateByIdForShare(templateId)),
        null,
        current ->
            Objects.equals(current.jobTemplateName(), name)
                && Objects.equals(current.metalakeId(), metalakeId));
  }

  private RuntimeException writeFailure(NameIdentifier ident, JobTemplatePO observed) {
    return OccWriteSupport.writeFailure(
        ident,
        Entity.EntityType.JOB_TEMPLATE,
        () ->
            SessionUtils.getWithoutCommit(
                JobTemplateMetaMapper.class,
                mapper -> mapper.selectJobTemplateByIdForUpdate(observed.jobTemplateId())),
        null,
        current ->
            Objects.equals(current.jobTemplateName(), observed.jobTemplateName())
                && Objects.equals(current.metalakeId(), observed.metalakeId()));
  }

  private void lockMetalake(String name, Long metalakeId) {
    OccWriteSupport.lockParentForChildWrite(
        name,
        Entity.EntityType.METALAKE,
        () ->
            SessionUtils.getWithoutCommit(
                MetalakeMetaMapper.class,
                mapper -> mapper.selectMetalakeMetaByIdForShare(metalakeId)),
        null,
        current -> Objects.equals(current.getMetalakeName(), name));
  }
}
