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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.JobPO;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;

/** Exercises job and template OCC against each configured relational backend. */
public class TestJobWriteOcc extends TestJDBCBackend {
  private static final String METALAKE = "job_occ";
  private final JobMetaService jobs = JobMetaService.getInstance();
  private final JobTemplateMetaService templates = JobTemplateMetaService.getInstance();
  private JobTemplateEntity template;
  private JobEntity job;
  private long metalakeId;

  /** A version-only change defeats stale updates and deletes without hiding the entity. */
  @TestTemplate
  public void testJobConflictsAndIdempotentDelete() throws IOException {
    initialize();
    JobPO observed = jobPO();
    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            jobs.<JobEntity>updateJob(
                jobIdent(),
                old -> {
                  Assertions.assertDoesNotThrow(
                      () -> jobs.updateJob(jobIdent(), current -> current));
                  return old;
                }));
    Assertions.assertEquals(observed.currentVersion() + 1, jobPO().currentVersion());
    Assertions.assertThrows(
        OptimisticLockException.class, () -> jobs.deleteJobWithVersion(jobIdent(), observed));
    Assertions.assertTrue(jobs.deleteJob(jobIdent()));
    Assertions.assertFalse(jobs.deleteJob(jobIdent()));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> jobs.updateJob(jobIdent(), current -> current));
  }

  /** A delete winning after the update read prevents resurrection. */
  @TestTemplate
  public void testDeleteWinsOverJobUpdate() throws IOException {
    initialize();
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            jobs.<JobEntity>updateJob(
                jobIdent(),
                old -> {
                  Assertions.assertTrue(jobs.deleteJob(jobIdent()));
                  return old;
                }));
    Assertions.assertFalse(jobs.deleteJob(jobIdent()));
  }

  /** Failed template CAS leaves child jobs untouched; a current delete removes both. */
  @TestTemplate
  public void testTemplateConflictsBeforeCascade() throws IOException {
    initialize();
    JobTemplatePO observed = templatePO();
    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            templates.<JobTemplateEntity>updateJobTemplate(
                templateIdent(),
                old -> {
                  Assertions.assertDoesNotThrow(
                      () -> templates.updateJobTemplate(templateIdent(), current -> current));
                  return old;
                }));
    Assertions.assertEquals(observed.currentVersion() + 1, templatePO().currentVersion());
    Assertions.assertThrows(
        OptimisticLockException.class,
        () -> templates.deleteJobTemplateWithVersion(templateIdent(), observed));
    Assertions.assertEquals(job.id(), jobs.getJobByIdentifier(jobIdent()).id());
    Assertions.assertTrue(templates.deleteJobTemplate(templateIdent()));
    Assertions.assertFalse(templates.deleteJobTemplate(templateIdent()));
    Assertions.assertTrue(jobs.listJobsByNamespace(NamespaceUtil.ofJob(METALAKE)).isEmpty());
  }

  /** Rollback restores the template and its already-deleted jobs. */
  @TestTemplate
  public void testCascadeRollback() throws IOException {
    initialize();
    JobTemplatePO observed = templatePO();
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> templates.deleteJobTemplateWithVersion(templateIdent(), observed),
                () -> {
                  throw new IllegalStateException("injected failure after cascade");
                }));
    Assertions.assertEquals(observed.currentVersion(), templatePO().currentVersion());
    Assertions.assertEquals(job.id(), jobs.getJobByIdentifier(jobIdent()).id());
  }

  /** A stale snapshot cannot delete a same-name replacement or its jobs. */
  @TestTemplate
  public void testSameNameRecreation() throws IOException {
    initialize();
    JobTemplatePO observed = templatePO();
    templates.deleteJobTemplate(templateIdent());
    template = TestJobTemplateMetaService.newShellJobTemplateEntity("template", "new", METALAKE);
    templates.insertJobTemplate(template, false);
    job = TestJobTemplateMetaService.newJobEntity("template", JobHandle.Status.QUEUED, METALAKE);
    jobs.insertJob(job, false);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> templates.deleteJobTemplateWithVersion(templateIdent(), observed));
    Assertions.assertEquals(job.id(), jobs.getJobByIdentifier(jobIdent()).id());
  }

  /** Job insertion waits for an in-flight template delete and then fails without inserting. */
  @TestTemplate
  public void testJobInsertFencedByTemplateDelete() throws Exception {
    initialize();
    JobTemplatePO observed = templatePO();
    JobEntity candidate =
        TestJobTemplateMetaService.newJobEntity("template", JobHandle.Status.QUEUED, METALAKE);
    Throwable failure =
        whileWriteUncommitted(
            () -> templates.deleteJobTemplateWithVersion(templateIdent(), observed),
            () -> jobs.insertJob(candidate, false));
    Assertions.assertInstanceOf(NoSuchEntityException.class, failure);
    Assertions.assertNull(
        SessionUtils.getWithoutCommit(
            JobMetaMapper.class,
            mapper -> mapper.selectJobRunIdForUpdate(candidate.id(), metalakeId)));
  }

  /** Metalake fencing rejects both kinds of insert after parent deletion commits. */
  @TestTemplate
  public void testInsertsFencedByMetalakeDelete() throws Exception {
    initialize();
    JobTemplateEntity candidate =
        TestJobTemplateMetaService.newShellJobTemplateEntity("other", "new", METALAKE);
    Throwable failure =
        whileWriteUncommitted(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId, 1L)),
            () -> templates.insertJobTemplate(candidate, false));
    Assertions.assertInstanceOf(NoSuchEntityException.class, failure);
    Assertions.assertThrows(NoSuchEntityException.class, () -> jobs.insertJob(job, false));
  }

  /** Missing templates are reported instead of silently dropping job insertion failures. */
  @TestTemplate
  public void testMissingTemplateInsertFails() throws IOException {
    initialize();
    JobEntity candidate =
        TestJobTemplateMetaService.newJobEntity("missing", JobHandle.Status.QUEUED, METALAKE);
    Assertions.assertThrows(NoSuchEntityException.class, () -> jobs.insertJob(candidate, false));
  }

  /** A parent delete waits for an in-flight insertion and preserves the committed active job. */
  @TestTemplate
  public void testTemplateDeleteWaitsForJobInsert() throws Exception {
    initialize();
    JobEntity candidate =
        TestJobTemplateMetaService.newJobEntity("template", JobHandle.Status.QUEUED, METALAKE);
    Throwable failure =
        whileWriteUncommitted(
            () -> Assertions.assertDoesNotThrow(() -> jobs.insertJob(candidate, false)),
            () -> templates.deleteJobTemplate(templateIdent()));
    Assertions.assertInstanceOf(NonEmptyEntityException.class, failure);
    Assertions.assertEquals(template.id(), templatePO().jobTemplateId());
    Assertions.assertEquals(1L, templatePO().currentVersion());
    Assertions.assertEquals(
        candidate.id(),
        jobs.getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE, candidate.name())).id());
  }

  /** An identifier under another metalake cannot delete a job solely by its numeric run ID. */
  @TestTemplate
  public void testDeleteChecksMetalakeIdentity() throws IOException {
    initialize();
    Assertions.assertFalse(jobs.deleteJob(NameIdentifierUtil.ofJob("other", job.name())));
    Assertions.assertEquals(job.id(), jobs.getJobByIdentifier(jobIdent()).id());
  }

  /** Renaming invalidates the old name while cascade cleanup continues to use stable IDs. */
  @TestTemplate
  public void testRenameAndCascadeIdentity() throws IOException {
    initialize();
    NameIdentifier oldIdent = templateIdent();
    JobTemplatePO observed = templatePO();
    template =
        templates.<JobTemplateEntity>updateJobTemplate(
            oldIdent,
            old ->
                JobTemplateEntity.builder()
                    .withId(old.id())
                    .withName("renamed")
                    .withNamespace(old.namespace())
                    .withComment(old.comment())
                    .withTemplateContent(old.templateContent())
                    .withAuditInfo(old.auditInfo())
                    .build());
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> templates.deleteJobTemplateWithVersion(oldIdent, observed));
    Assertions.assertEquals("renamed", jobs.getJobByIdentifier(jobIdent()).jobTemplateName());
    Assertions.assertTrue(templates.deleteJobTemplate(templateIdent()));
    Assertions.assertNull(
        SessionUtils.getWithoutCommit(
            JobMetaMapper.class, mapper -> mapper.selectJobRunIdForUpdate(job.id(), metalakeId)));
  }

  /** All nonterminal states reject deletion and roll back the root CAS. */
  @TestTemplate
  public void testNonterminalJobsPreventTemplateDeletion() throws IOException {
    initialize();
    for (JobHandle.Status status :
        new JobHandle.Status[] {
          JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING
        }) {
      JobEntity active = TestJobTemplateMetaService.newJobEntity("template", status, METALAKE);
      jobs.insertJob(active, false);
      Assertions.assertThrows(
          NonEmptyEntityException.class, () -> templates.deleteJobTemplate(templateIdent()));
      Assertions.assertEquals(1L, templatePO().currentVersion());
      Assertions.assertEquals(job.id(), jobs.getJobByIdentifier(jobIdent()).id());
      Assertions.assertEquals(
          active.id(),
          jobs.getJobByIdentifier(NameIdentifierUtil.ofJob(METALAKE, active.name())).id());
      Assertions.assertTrue(jobs.deleteJob(NameIdentifierUtil.ofJob(METALAKE, active.name())));
    }
    for (JobHandle.Status status :
        new JobHandle.Status[] {JobHandle.Status.CANCELLED, JobHandle.Status.FAILED}) {
      jobs.insertJob(TestJobTemplateMetaService.newJobEntity("template", status, METALAKE), false);
    }
    Assertions.assertTrue(templates.deleteJobTemplate(templateIdent()));
    Assertions.assertTrue(jobs.listJobsByNamespace(NamespaceUtil.ofJob(METALAKE)).isEmpty());
  }

  private void initialize() throws IOException {
    metalakeId = RandomIdGenerator.INSTANCE.nextId();
    backend.insert(createBaseMakeLake(metalakeId, METALAKE, AUDIT_INFO), false);
    template =
        TestJobTemplateMetaService.newShellJobTemplateEntity("template", "original", METALAKE);
    templates.insertJobTemplate(template, false);
    job = TestJobTemplateMetaService.newJobEntity("template", JobHandle.Status.SUCCEEDED, METALAKE);
    jobs.insertJob(job, false);
  }

  private NameIdentifier jobIdent() {
    return NameIdentifierUtil.ofJob(METALAKE, job.name());
  }

  private NameIdentifier templateIdent() {
    return NameIdentifierUtil.ofJobTemplate(METALAKE, template.name());
  }

  private JobPO jobPO() {
    return SessionUtils.getWithoutCommit(
        JobMetaMapper.class, mapper -> mapper.selectJobPOByMetalakeAndRunId(METALAKE, job.id()));
  }

  private JobTemplatePO templatePO() {
    return SessionUtils.getWithoutCommit(
        JobTemplateMetaMapper.class, mapper -> mapper.selectJobTemplateById(template.id()));
  }

  private Throwable whileWriteUncommitted(Runnable write, Executable victim) throws Exception {
    CountDownLatch locked = new CountDownLatch(1);
    CountDownLatch commit = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> writer =
          executor.submit(
              () ->
                  SessionUtils.doMultipleWithCommit(
                      write,
                      () -> {
                        locked.countDown();
                        try {
                          Assertions.assertTrue(commit.await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                      }));
      Assertions.assertTrue(locked.await(30, TimeUnit.SECONDS));
      Future<Throwable> reader =
          executor.submit(
              () -> {
                started.countDown();
                try {
                  victim.execute();
                  return null;
                } catch (Throwable t) {
                  return t;
                }
              });
      Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
      Assertions.assertThrows(TimeoutException.class, () -> reader.get(500, TimeUnit.MILLISECONDS));
      commit.countDown();
      writer.get(30, TimeUnit.SECONDS);
      return reader.get(30, TimeUnit.SECONDS);
    } finally {
      commit.countDown();
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }
}
