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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobTemplateResolver {

  @Test
  public void testCreateShellRuntimeJobTemplate() {
    String testScript1 = "https://repo.example.com/jobs/testScript1.sh";
    String testScript2 = "https://repo.example.com/jobs/testScript2.sh";

    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName("testShellJob")
            .withComment("This is a test shell job template")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}, {{arg4}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withScripts(Lists.newArrayList(testScript1, testScript2))
            .build();

    JobTemplateEntity entity =
        JobTemplateEntity.builder()
            .withId(1L)
            .withName(shellJobTemplate.name())
            .withComment(shellJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplate result =
        new JobTemplateResolver(entity)
            .resolve(
                ImmutableMap.of(
                    "arg3", "value3",
                    "arg4", "value4",
                    "val1", "value1",
                    "val2", "value2",
                    "customVal1", "customValue1"));

    Assertions.assertEquals(shellJobTemplate.name(), result.name());
    Assertions.assertEquals(shellJobTemplate.comment(), result.comment());
    Assertions.assertEquals("/bin/echo", result.executable());
    Assertions.assertEquals(
        Lists.newArrayList("arg1", "arg2", "value3, value4"), result.arguments());
    Assertions.assertEquals(
        ImmutableMap.of("ENV_VAR1", "value1", "ENV_VAR2", "value2"), result.environments());
    Assertions.assertEquals(ImmutableMap.of("customField1", "customValue1"), result.customFields());

    Assertions.assertEquals(2, ((ShellJobTemplate) result).scripts().size());
    Assertions.assertEquals(
        Lists.newArrayList(testScript1, testScript2), ((ShellJobTemplate) result).scripts());
  }

  @Test
  public void testCreateShellRuntimeJobTemplateWithReplacementsInScripts() {
    String testScript1 = "https://repo.example.com/jobs/testScript1.sh";
    String testScript2 = "https://repo.example.com/jobs/testScript2.sh";

    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName("testShellJob1")
            .withComment("This is a test shell job template")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}, {{arg4}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withScripts(
                Lists.newArrayList(
                    testScript1.replace("testScript1", "{{scriptName1}}"),
                    testScript2.replace("testScript2", "{{scriptName2}}")))
            .build();

    JobTemplateEntity entity =
        JobTemplateEntity.builder()
            .withId(1L)
            .withName(shellJobTemplate.name())
            .withComment(shellJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplate result =
        new JobTemplateResolver(entity)
            .resolve(
                ImmutableMap.of(
                    "arg3", "value3",
                    "arg4", "value4",
                    "val1", "value1",
                    "val2", "value2",
                    "customVal1", "customValue1",
                    "scriptName1", "testScript1",
                    "scriptName2", "testScript2"));

    Assertions.assertEquals("/bin/echo", result.executable());
    Assertions.assertEquals(2, ((ShellJobTemplate) result).scripts().size());
    Assertions.assertEquals(
        Lists.newArrayList(testScript1, testScript2), ((ShellJobTemplate) result).scripts());
  }

  @Test
  public void testCreateSparkRuntimeJobTemplate() {
    String executable = "https://repo.example.com/jobs/testSparkJob.jar";
    String jar1 = "https://repo.example.com/jobs/testJar1.jar";
    String jar2 = "https://repo.example.com/jobs/testJar2.jar";

    String file1 = "https://repo.example.com/jobs/testFile1.txt";
    String file2 = "https://repo.example.com/jobs/testFile2.txt";

    String archive1 = "https://repo.example.com/jobs/testArchive1.zip";

    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName("testSparkJob")
            .withComment("This is a test Spark job template")
            .withExecutable(executable)
            .withClassName("org.apache.gravitino.TestSparkJob")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withJars(Lists.newArrayList(jar1, jar2))
            .withFiles(Lists.newArrayList(file1, file2))
            .withArchives(Lists.newArrayList(archive1))
            .withConfigs(
                ImmutableMap.of(
                    "spark.executor.memory",
                    "{{executor-mem}}",
                    "spark.driver.cores",
                    "{{driver-cores}}"))
            .build();

    JobTemplateEntity entity =
        JobTemplateEntity.builder()
            .withId(1L)
            .withName(sparkJobTemplate.name())
            .withComment(sparkJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplate result =
        new JobTemplateResolver(entity)
            .resolve(
                ImmutableMap.of(
                    "arg3", "value3",
                    "val1", "value1",
                    "val2", "value2",
                    "customVal1", "customValue1",
                    "executor-mem", "4g",
                    "driver-cores", "2"));

    Assertions.assertEquals(sparkJobTemplate.name(), result.name());
    Assertions.assertEquals(sparkJobTemplate.comment(), result.comment());
    Assertions.assertEquals(executable, result.executable());
    Assertions.assertEquals(Lists.newArrayList("arg1", "arg2", "value3"), result.arguments());
    Assertions.assertEquals(
        ImmutableMap.of("ENV_VAR1", "value1", "ENV_VAR2", "value2"), result.environments());
    Assertions.assertEquals(ImmutableMap.of("customField1", "customValue1"), result.customFields());

    Assertions.assertEquals(2, ((SparkJobTemplate) result).jars().size());
    Assertions.assertEquals(Lists.newArrayList(jar1, jar2), ((SparkJobTemplate) result).jars());

    Assertions.assertEquals(2, ((SparkJobTemplate) result).files().size());
    Assertions.assertEquals(Lists.newArrayList(file1, file2), ((SparkJobTemplate) result).files());

    Assertions.assertEquals(1, ((SparkJobTemplate) result).archives().size());
    Assertions.assertEquals(Lists.newArrayList(archive1), ((SparkJobTemplate) result).archives());

    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g", "spark.driver.cores", "2"),
        ((SparkJobTemplate) result).configs());
  }

  @Test
  public void testCreateSparkRuntimeJobTemplateWithReplacements() {
    String executable = "https://repo.example.com/jobs/testSparkJob.jar";
    String jar1 = "https://repo.example.com/jobs/testJar1.jar";
    String jar2 = "https://repo.example.com/jobs/testJar2.jar";

    String file1 = "https://repo.example.com/jobs/testFile1.txt";
    String file2 = "https://repo.example.com/jobs/testFile2.txt";

    String archive1 = "https://repo.example.com/jobs/testArchive1.zip";

    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName("testSparkJob")
            .withComment("This is a test Spark job template")
            .withExecutable(executable.replace("test", "{{env}}"))
            .withClassName("org.apache.gravitino.TestSparkJob")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withJars(
                Lists.newArrayList(
                    jar1.replace("test", "{{env}}"), jar2.replace("test", "{{env}}")))
            .withFiles(
                Lists.newArrayList(
                    file1.replace("test", "{{env}}"), file2.replace("test", "{{env}}")))
            .withArchives(Lists.newArrayList(archive1.replace("test", "{{env}}")))
            .withConfigs(
                ImmutableMap.of(
                    "spark.executor.memory",
                    "{{executor-mem}}",
                    "spark.driver.cores",
                    "{{driver-cores}}"))
            .build();

    JobTemplateEntity entity =
        JobTemplateEntity.builder()
            .withId(1L)
            .withName(sparkJobTemplate.name())
            .withComment(sparkJobTemplate.comment())
            .withNamespace(NamespaceUtil.ofJobTemplate("test"))
            .withTemplateContent(
                JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobTemplate result =
        new JobTemplateResolver(entity)
            .resolve(
                ImmutableMap.of(
                    "arg3", "value3",
                    "val1", "value1",
                    "val2", "value2",
                    "customVal1", "customValue1",
                    "executor-mem", "4g",
                    "driver-cores", "2",
                    "env", "test"));

    Assertions.assertEquals(executable, result.executable());
    Assertions.assertEquals(Lists.newArrayList("arg1", "arg2", "value3"), result.arguments());
    Assertions.assertEquals(
        ImmutableMap.of("ENV_VAR1", "value1", "ENV_VAR2", "value2"), result.environments());
    Assertions.assertEquals(ImmutableMap.of("customField1", "customValue1"), result.customFields());

    Assertions.assertEquals(2, ((SparkJobTemplate) result).jars().size());
    Assertions.assertEquals(Lists.newArrayList(jar1, jar2), ((SparkJobTemplate) result).jars());

    Assertions.assertEquals(2, ((SparkJobTemplate) result).files().size());
    Assertions.assertEquals(Lists.newArrayList(file1, file2), ((SparkJobTemplate) result).files());

    Assertions.assertEquals(1, ((SparkJobTemplate) result).archives().size());
    Assertions.assertEquals(Lists.newArrayList(archive1), ((SparkJobTemplate) result).archives());
  }

  @Test
  public void testCheckJobConf() {
    JobTemplateEntity entity =
        shellTemplateEntity(Lists.newArrayList("{{table}}", "{{target}}", "{{mode:-full}}"));

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new JobTemplateResolver(entity).checkJobConf(ImmutableMap.of("table", "t")));
    Assertions.assertTrue(e.getMessage().contains("[target]"), e.getMessage());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JobTemplateResolver(entity).checkJobConf(null));

    // Optional parameters and unused keys don't fail the check.
    Assertions.assertDoesNotThrow(
        () ->
            new JobTemplateResolver(entity)
                .checkJobConf(ImmutableMap.of("table", "t", "target", "", "unused", "x")));
  }

  @Test
  public void testCreateUsesDefaultValues() {
    JobTemplateEntity entity =
        shellTemplateEntity(
            Lists.newArrayList("--table", "{{table}}", "--mode", "{{mode:-full}}", "{{note:-}}"));

    JobTemplate result = new JobTemplateResolver(entity).resolve(ImmutableMap.of("table", "t"));
    Assertions.assertEquals(
        Lists.newArrayList("--table", "t", "--mode", "full", ""), result.arguments());
  }

  @Test
  public void testCreateFailsOnMissingParameters() {
    JobTemplateEntity entity = shellTemplateEntity(Lists.newArrayList("{{table}}"));

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new JobTemplateResolver(entity).resolve(ImmutableMap.of()));
    Assertions.assertTrue(e.getMessage().contains("table"), e.getMessage());
  }

  @Test
  public void testResolveKeepsResourceUris() {
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("spark_job")
            .withExecutable("https://repo.example.com/{{version}}/app.jar")
            .withClassName("org.example.App")
            .withJars(Lists.newArrayList("s3a://bucket/lib-{{version}}.jar"))
            .withFiles(Lists.newArrayList("hdfs://nn/conf/app.conf"))
            .build();

    SparkJobTemplate result =
        (SparkJobTemplate)
            new JobTemplateResolver(toEntity(template)).resolve(ImmutableMap.of("version", "1.0"));

    Assertions.assertEquals("https://repo.example.com/1.0/app.jar", result.executable());
    Assertions.assertEquals(Lists.newArrayList("s3a://bucket/lib-1.0.jar"), result.jars());
    Assertions.assertEquals(Lists.newArrayList("hdfs://nn/conf/app.conf"), result.files());
  }

  @Test
  public void testCreateRejectsDuplicateKeysAfterResolution() {
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("duplicate_keys")
            .withExecutable("/bin/echo")
            .withEnvironments(ImmutableMap.of("{{a}}", "1", "{{b}}", "2"))
            .build();
    JobTemplateEntity entity = toEntity(template);

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new JobTemplateResolver(entity).resolve(ImmutableMap.of("a", "SAME", "b", "SAME")));
    Assertions.assertTrue(e.getMessage().contains("SAME"), e.getMessage());
  }

  private static JobTemplateEntity shellTemplateEntity(List<String> arguments) {
    return toEntity(
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable("/bin/echo")
            .withArguments(arguments)
            .build());
  }

  private static JobTemplateEntity toEntity(JobTemplate template) {
    return JobTemplateEntity.builder()
        .withId(1L)
        .withName(template.name())
        .withNamespace(NamespaceUtil.ofJobTemplate("test"))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(template))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
