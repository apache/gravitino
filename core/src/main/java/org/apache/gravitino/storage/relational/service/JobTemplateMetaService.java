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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.JobTemplateEntity;
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

  public JobTemplateEntity getJobTemplateByIdentifier(NameIdentifier jobTemplateIdent) {
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

    return JobTemplatePO.fromJobTemplatePO(jobTemplatePO, jobTemplateIdent.namespace());
  }

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

  public int deleteJobTemplatesByLegacyTimeline(long legacyTimeline, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        JobTemplateMetaMapper.class,
        mapper -> mapper.deleteJobTemplateMetasByLegacyTimeline(legacyTimeline, limit));
  }
}
