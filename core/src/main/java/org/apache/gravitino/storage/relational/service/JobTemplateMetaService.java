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
import java.util.concurrent.atomic.AtomicInteger;
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
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

      JobTemplatePO.JobTemplatePOBuilder builder =
          JobTemplatePO.builder().withMetalakeId(metalakeId);
      JobTemplatePO jobTemplatePO =
          JobTemplatePO.initializeJobTemplatePO(jobTemplateEntity, builder);

      SessionUtils.doWithCommit(
          JobTemplateMetaMapper.class,
          mapper -> {
            if (overwrite) {
              mapper.insertJobTemplateMetaOnDuplicateKeyUpdate(jobTemplatePO);
            } else {
              mapper.insertJobTemplateMeta(jobTemplatePO);
            }
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.JOB_TEMPLATE, jobTemplateEntity.name());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteJobTemplate")
  public boolean deleteJobTemplate(NameIdentifier jobTemplateIdent) {
    String metalakeName = jobTemplateIdent.namespace().level(0);
    String jobTemplateName = jobTemplateIdent.name();

    AtomicInteger result = new AtomicInteger(0);
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                JobMetaMapper.class,
                mapper ->
                    mapper.softDeleteJobMetaByMetalakeAndTemplate(metalakeName, jobTemplateName)),
        () ->
            result.set(
                SessionUtils.getWithoutCommit(
                    JobTemplateMetaMapper.class,
                    mapper ->
                        mapper.softDeleteJobTemplateMetaByMetalakeAndName(
                            metalakeName, jobTemplateName))));
    return result.get() > 0;
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

    Integer result;
    try {
      result =
          SessionUtils.doWithCommitAndFetchResult(
              JobTemplateMetaMapper.class,
              mapper -> mapper.updateJobTemplateMeta(newJobTemplatePO, oldJobTemplatePO));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(
          e, Entity.EntityType.JOB_TEMPLATE, oldJobTemplateEntity.name());
      throw e;
    }

    if (result == null || result == 0) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.JOB_TEMPLATE.name().toLowerCase(Locale.ROOT),
          oldJobTemplateEntity.name());
    } else if (result > 1) {
      throw new IOException(
          String.format(
              "Failed to update job template: %s, because more than one rows are updated: %d",
              oldJobTemplateEntity.name(), result));
    } else {
      return newJobTemplateEntity;
    }
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
}
