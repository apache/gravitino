/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JobAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static ShellJobTemplate.Builder builder;
  private static final String ROLE = "job_test_role";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    testStagingDir = Files.createTempDirectory("test_staging_dir").toFile();
    String testEntryScriptPath = generateTestEntryScript();
    String testLibScriptPath = generateTestLibScript();

    builder =
        ShellJobTemplate.builder()
            .withComment("Test shell job template")
            .withExecutable(testEntryScriptPath)
            .withArguments(Lists.newArrayList("{{arg1}}", "{{arg2}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR", "{{env_var}}"))
            .withScripts(Lists.newArrayList(testLibScriptPath))
            .withCustomFields(Collections.emptyMap());

    Map<String, String> configs =
        ImmutableMap.of(
            "gravitino.job.stagingDir",
            testStagingDir.getAbsolutePath(),
            "gravitino.job.statusPullIntervalInMs",
            "3000");
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    // Create role for authorization tests
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.createRole(ROLE, new HashMap<>(), Collections.emptyList());
    metalake.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);
  }

  @Test
  @Order(1)
  public void testRegisterJobTemplate() {
    // User without privilege cannot register job template
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserClient
                .loadMetalake(METALAKE)
                .registerJobTemplate(builder.withName("test1").build()));

    // Grant privilege to normal user
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.RegisterJobTemplate.allow()));

    // Now normal user can register job templates
    JobTemplate template1 = builder.withName("test_1").build();
    JobTemplate template2 = builder.withName("test_2").build();
    normalUserClient.loadMetalake(METALAKE).registerJobTemplate(template1);
    normalUserClient.loadMetalake(METALAKE).registerJobTemplate(template2);

    // Admin can always register job templates
    JobTemplate template3 = builder.withName("test_3").build();
    metalake.registerJobTemplate(template3);
  }

  @Test
  @Order(2)
  public void testListJobTemplates() {
    // Normal user can see job templates they own (test_1, test_2)
    List<JobTemplate> normalUserTemplates =
        normalUserClient.loadMetalake(METALAKE).listJobTemplates();
    Assertions.assertTrue(normalUserTemplates.stream().anyMatch(s -> s.name().equals("test_1")));
    Assertions.assertTrue(normalUserTemplates.stream().anyMatch(s -> s.name().equals("test_2")));

    // Admin can see all job templates (test_1, test_2, test_3)
    List<JobTemplate> adminTemplates = client.loadMetalake(METALAKE).listJobTemplates();
    Assertions.assertTrue(adminTemplates.stream().anyMatch(s -> s.name().equals("test_1")));
    Assertions.assertTrue(adminTemplates.stream().anyMatch(s -> s.name().equals("test_2")));
    Assertions.assertTrue(adminTemplates.stream().anyMatch(s -> s.name().equals("test_3")));
  }

  @Test
  @Order(3)
  public void testGetJobTemplate() {
    // Normal user can get their own job template
    JobTemplate template = normalUserClient.loadMetalake(METALAKE).getJobTemplate("test_1");
    Assertions.assertNotNull(template);
    Assertions.assertEquals("test_1", template.name());

    // Normal user cannot get job template owned by admin
    assertThrows(
        ForbiddenException.class,
        () -> normalUserClient.loadMetalake(METALAKE).getJobTemplate("test_3"));

    // Admin can get any job template
    JobTemplate adminTemplate = client.loadMetalake(METALAKE).getJobTemplate("test_3");
    Assertions.assertNotNull(adminTemplate);
  }

  @Test
  @Order(4)
  public void testDeleteJobTemplate() {
    // Normal user cannot delete job template owned by admin
    assertThrows(
        ForbiddenException.class,
        () -> normalUserClient.loadMetalake(METALAKE).deleteJobTemplate("test_3"));

    // Normal user can delete their own job template
    boolean deleted = normalUserClient.loadMetalake(METALAKE).deleteJobTemplate("test_1");
    Assertions.assertTrue(deleted);

    // Admin can delete any job template
    boolean adminDeleted = client.loadMetalake(METALAKE).deleteJobTemplate("test_3");
    Assertions.assertTrue(adminDeleted);
  }

  @Test
  @Order(5)
  public void testRunJob() {
    // Normal user without RunJob privilege cannot run jobs
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserClient
                .loadMetalake(METALAKE)
                .runJob(
                    "test_2",
                    ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2")));

    // Grant RunJob privilege to normal user but not UseJobTemplate privilege
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.RunJob.allow()));

    // User with RunJob privilege but without UseJobTemplate privilege cannot run jobs
    // The authorization expression is: METALAKE::OWNER || (METALAKE::RUN_JOB &&
    // (ANY_USE_JOB_TEMPLATE || JOB_TEMPLATE::OWNER))
    client.loadMetalake(METALAKE).registerJobTemplate(builder.withName("test_4").build());
    assertThrows(
        ForbiddenException.class,
        () ->
            normalUserClient
                .loadMetalake(METALAKE)
                .runJob(
                    "test_4",
                    ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2")));

    // Grant UseJobTemplate privilege on the metalake to normal user
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(null, "test_2", MetadataObject.Type.JOB_TEMPLATE),
        ImmutableList.of(Privileges.UseJobTemplate.allow()));
    Assertions.assertDoesNotThrow(() -> metalake.getRole(ROLE));

    // Now normal user can run jobs on their own template (has both RunJob and UseJobTemplate)
    JobHandle normalUserJobHandle =
        normalUserClient
            .loadMetalake(METALAKE)
            .runJob(
                "test_2",
                ImmutableMap.of("arg1", "value1", "arg2", "success", "env_var", "value2"));
    Assertions.assertNotNull(normalUserJobHandle);
    Assertions.assertEquals("test_2", normalUserJobHandle.jobTemplateName());

    // Admin can run jobs on any template
    JobHandle adminJobHandle =
        metalake.runJob(
            "test_2", ImmutableMap.of("arg1", "value3", "arg2", "success", "env_var", "value4"));
    Assertions.assertNotNull(adminJobHandle);
  }

  @Test
  @Order(6)
  public void testListJob() {
    // Normal user can see jobs they own (1 job from test_2 template)
    List<JobHandle> normalUserJobs = normalUserClient.loadMetalake(METALAKE).listJobs();
    Assertions.assertEquals(1, normalUserJobs.size());

    // Admin can see all jobs (2 jobs total)
    List<JobHandle> adminJobs = client.loadMetalake(METALAKE).listJobs();
    Assertions.assertEquals(2, adminJobs.size());

    // Listing jobs for specific template shows all jobs from that template
    List<JobHandle> test2Jobs = normalUserClient.loadMetalake(METALAKE).listJobs("test_2");
    Assertions.assertEquals(1, test2Jobs.size());
  }

  @Test
  @Order(7)
  public void testGetJob() {
    // Get job IDs for testing
    List<JobHandle> normalUserJobs = normalUserClient.loadMetalake(METALAKE).listJobs();
    Assertions.assertTrue(normalUserJobs.size() > 0);
    String normalUserJobId = normalUserJobs.get(0).jobId();

    List<JobHandle> adminJobs = client.loadMetalake(METALAKE).listJobs();
    String adminJobId = null;
    for (JobHandle job : adminJobs) {
      if (!job.jobId().equals(normalUserJobId)) {
        adminJobId = job.jobId();
        break;
      }
    }
    Assertions.assertNotNull(adminJobId);

    // Normal user can get their own job
    JobHandle job = normalUserClient.loadMetalake(METALAKE).getJob(normalUserJobId);
    Assertions.assertNotNull(job);
    Assertions.assertEquals(normalUserJobId, job.jobId());

    // Normal user cannot get job owned by admin
    String finalAdminJobId = adminJobId;
    assertThrows(
        ForbiddenException.class,
        () -> normalUserClient.loadMetalake(METALAKE).getJob(finalAdminJobId));

    // Admin can get any job
    JobHandle adminJob = client.loadMetalake(METALAKE).getJob(adminJobId);
    Assertions.assertNotNull(adminJob);
  }

  @Test
  @Order(8)
  public void testCancelJob() {
    // Get job IDs for testing
    List<JobHandle> normalUserJobs = normalUserClient.loadMetalake(METALAKE).listJobs();
    Assertions.assertTrue(normalUserJobs.size() > 0);
    String normalUserJobId = normalUserJobs.get(0).jobId();

    List<JobHandle> adminJobs = client.loadMetalake(METALAKE).listJobs();
    String adminJobId = null;
    for (JobHandle job : adminJobs) {
      if (!job.jobId().equals(normalUserJobId)) {
        adminJobId = job.jobId();
        break;
      }
    }
    Assertions.assertNotNull(adminJobId);

    // Normal user cannot cancel job owned by admin
    String finalAdminJobId = adminJobId;
    assertThrows(
        ForbiddenException.class,
        () -> normalUserClient.loadMetalake(METALAKE).cancelJob(finalAdminJobId));

    // Normal user can cancel their own job
    JobHandle canceledJob = normalUserClient.loadMetalake(METALAKE).cancelJob(normalUserJobId);
    Assertions.assertNotNull(canceledJob);

    // Admin can cancel any job
    JobHandle adminCanceledJob = client.loadMetalake(METALAKE).cancelJob(adminJobId);
    Assertions.assertNotNull(adminCanceledJob);
  }

  @Test
  @Order(9)
  public void testJobOperationsWithNonExistentMetalake() throws Exception {
    // Test that all job operations with @AuthorizationExpression return 403 Forbidden
    // when the metalake doesn't exist, instead of inconsistent 404 responses
    String nonExistentMetalake = "nonExistentMetalake";

    // Access the restClient from normalUserClient using reflection
    Method restClientMethod =
        normalUserClient.getClass().getSuperclass().getDeclaredMethod("restClient");
    restClientMethod.setAccessible(true);
    Object restClient = restClientMethod.invoke(normalUserClient);

    // Create a MetalakeDTO for the non-existent metalake
    MetalakeDTO metalakeDTO =
        MetalakeDTO.builder()
            .withName(nonExistentMetalake)
            .withComment("test")
            .withProperties(Maps.newHashMap())
            .withAudit(
                org.apache.gravitino.dto.AuditDTO.builder()
                    .withCreator("test")
                    .withCreateTime(java.time.Instant.now())
                    .build())
            .build();

    // Use DTOConverters.toMetaLake() via reflection to create GravitinoMetalake
    Class<?> dtoConvertersClass = Class.forName("org.apache.gravitino.client.DTOConverters");
    Method toMetaLakeMethod =
        dtoConvertersClass.getDeclaredMethod(
            "toMetaLake",
            MetalakeDTO.class,
            Class.forName("org.apache.gravitino.client.RESTClient"));
    toMetaLakeMethod.setAccessible(true);
    GravitinoMetalake nonExistentMetalakeObj =
        (GravitinoMetalake) toMetaLakeMethod.invoke(null, metalakeDTO, restClient);

    // Test registerJobTemplate - should return 403 ForbiddenException
    assertThrows(
        ForbiddenException.class,
        () -> nonExistentMetalakeObj.registerJobTemplate(builder.withName("testTemplate").build()));

    // Test listJobTemplates - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, nonExistentMetalakeObj::listJobTemplates);

    // Test getJobTemplate - should return 403 ForbiddenException
    assertThrows(
        ForbiddenException.class, () -> nonExistentMetalakeObj.getJobTemplate("testTemplate"));

    // Test deleteJobTemplate - should return 403 ForbiddenException
    assertThrows(
        ForbiddenException.class, () -> nonExistentMetalakeObj.deleteJobTemplate("testTemplate"));

    // Test runJob - should return 403 ForbiddenException
    assertThrows(
        ForbiddenException.class,
        () -> nonExistentMetalakeObj.runJob("testTemplate", Maps.newHashMap()));

    // Test listJobs - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, nonExistentMetalakeObj::listJobs);

    // Test listJobs with template name - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, () -> nonExistentMetalakeObj.listJobs("testTemplate"));

    // Test getJob - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, () -> nonExistentMetalakeObj.getJob("testJobId"));

    // Test cancelJob - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, () -> nonExistentMetalakeObj.cancelJob("testJobId"));
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (testStagingDir != null && testStagingDir.exists()) {
      FileUtils.deleteDirectory(testStagingDir);
    }
  }
}
