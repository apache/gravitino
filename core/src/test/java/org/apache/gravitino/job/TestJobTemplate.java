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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobTemplate {

  private static File tempDir;
  private File tempStagingDir;

  @BeforeAll
  public static void setUpClass() throws IOException {
    // Create a temporary directory for testing
    tempDir = Files.createTempDirectory("job-template-test").toFile();
  }

  @AfterAll
  public static void tearDownClass() throws IOException {
    // Clean up the temporary directory after all tests
    if (tempDir != null && tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir);
      tempDir = null;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    // Create a temporary staging directory for each test
    tempStagingDir = Files.createTempDirectory(tempDir.toPath(), "staging").toFile();
  }

  @AfterEach
  public void tearDown() throws IOException {
    // Clean up the temporary staging directory after each test
    if (tempStagingDir != null && tempStagingDir.exists()) {
      FileUtils.deleteDirectory(tempStagingDir);
      tempStagingDir = null;
    }
  }

  @Test
  public void testReplacePlaceholders() {
    String template = "Hello, {{name}}! Welcome to {{place}}.";
    Map<String, String> replacements = ImmutableMap.of("name", "Alice", "place", "Wonderland");

    String result = JobManager.replacePlaceholder(template, replacements);
    Assertions.assertEquals("Hello, Alice! Welcome to Wonderland.", result);

    // Test with missing replacements
    replacements = ImmutableMap.of("name", "Bob");
    result = JobManager.replacePlaceholder(template, replacements);
    Assertions.assertEquals("Hello, Bob! Welcome to {{place}}.", result);

    // Test with no replacements
    result = JobManager.replacePlaceholder(template, ImmutableMap.of());
    Assertions.assertEquals("Hello, {{name}}! Welcome to {{place}}.", result);

    // Test with no placeholders
    String noPlaceholders = "Hello, World!";
    result = JobManager.replacePlaceholder(noPlaceholders, replacements);
    Assertions.assertEquals("Hello, World!", result);

    // Test with repeated placeholders
    String repeatedTemplate = "Hello, {{name}}! Your name is {{name}}.";
    replacements = ImmutableMap.of("name", "Charlie");
    result = JobManager.replacePlaceholder(repeatedTemplate, replacements);
    Assertions.assertEquals("Hello, Charlie! Your name is Charlie.", result);

    // Test with incomplete placeholders
    String incompleteTemplate = "Hello, {{name}! Welcome to {{place}}.";
    replacements = ImmutableMap.of("name", "Dave", "place", "Earth");
    result = JobManager.replacePlaceholder(incompleteTemplate, replacements);
    Assertions.assertEquals("Hello, {{name}! Welcome to Earth.", result);

    // Test with empty template
    String emptyTemplate = "";
    result = JobManager.replacePlaceholder(emptyTemplate, replacements);
    Assertions.assertEquals("", result);

    // Test with nested braces
    String nestedTemplate = "Hello, {{name}}! Your code is {{{value}}}.";
    replacements = ImmutableMap.of("name", "Eve", "value", "42");
    result = JobManager.replacePlaceholder(nestedTemplate, replacements);
    Assertions.assertEquals("Hello, Eve! Your code is {42}.", result);

    nestedTemplate = "Hello, {{name}}! Your code is {{{{value}}}}.";
    result = JobManager.replacePlaceholder(nestedTemplate, replacements);
    Assertions.assertEquals("Hello, Eve! Your code is {{42}}.", result);

    // Test with special characters in placeholders.
    String specialTemplate = "Hello, {{name}}! Your score is {{score%}}.";
    replacements = ImmutableMap.of("name", "Frank", "score%", "100%");
    result = JobManager.replacePlaceholder(specialTemplate, replacements);
    Assertions.assertEquals("Hello, Frank! Your score is {{score%}}.", result);

    // Test with alphanumeric placeholders
    String alphanumericTemplate = "Hello, {{user_name}}! Your score is {{score123}}.";
    replacements = ImmutableMap.of("user_name", "Grace", "score123", "200");
    result = JobManager.replacePlaceholder(alphanumericTemplate, replacements);
    Assertions.assertEquals("Hello, Grace! Your score is 200.", result);

    // Test with "." and "-"
    String dotDashTemplate = "Hello, {{user.name}}! Your score is {{score-123}}.";
    replacements = ImmutableMap.of("user.name", "Hank", "score-123", "300");
    result = JobManager.replacePlaceholder(dotDashTemplate, replacements);
    Assertions.assertEquals("Hello, Hank! Your score is 300.", result);
  }

  @Test
  public void testFetchFilesFromUir() throws IOException {
    File testFile1 = Files.createTempFile(tempDir.toPath(), "testFile1", ".txt").toFile();
    String result =
        JobManager.fetchFileFromUri(testFile1.toURI().toString(), tempStagingDir, 30 * 1000);
    File resultFile = new File(result);
    Assertions.assertEquals(testFile1.getName(), resultFile.getName());

    File testFile2 = Files.createTempFile(tempDir.toPath(), "testFile2", ".txt").toFile();
    File testFile3 = Files.createTempFile(tempDir.toPath(), "testFile3", ".txt").toFile();

    List<String> expectedUris =
        Lists.newArrayList(testFile2.toURI().toString(), testFile3.toURI().toString());
    List<String> resultUris = JobManager.fetchFilesFromUri(expectedUris, tempStagingDir, 30 * 1000);

    Assertions.assertEquals(2, resultUris.size());
    List<String> resultFileNames =
        resultUris.stream().map(uri -> new File(uri).getName()).collect(Collectors.toList());
    Assertions.assertTrue(resultFileNames.contains(testFile2.getName()));
    Assertions.assertTrue(resultFileNames.contains(testFile3.getName()));
  }

  @Test
  public void testCreateShellRuntimeJobTemplate() throws IOException {
    File testScript1 = Files.createTempFile(tempDir.toPath(), "testScript1", ".sh").toFile();
    File testScript2 = Files.createTempFile(tempDir.toPath(), "testScript2", ".sh").toFile();

    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName("testShellJob")
            .withComment("This is a test shell job template")
            .withExecutable("/bin/echo")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}, {{arg4}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withScripts(
                Lists.newArrayList(testScript1.toURI().toString(), testScript2.toURI().toString()))
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
        JobManager.createRuntimeJobTemplate(
            entity,
            ImmutableMap.of(
                "arg3", "value3",
                "arg4", "value4",
                "val1", "value1",
                "val2", "value2",
                "customVal1", "customValue1"),
            tempStagingDir);

    Assertions.assertEquals(shellJobTemplate.name(), result.name());
    Assertions.assertEquals(shellJobTemplate.comment(), result.comment());
    Assertions.assertEquals("echo", new File(result.executable).getName());
    Assertions.assertEquals(
        Lists.newArrayList("arg1", "arg2", "value3, value4"), result.arguments());
    Assertions.assertEquals(
        ImmutableMap.of("ENV_VAR1", "value1", "ENV_VAR2", "value2"), result.environments());
    Assertions.assertEquals(ImmutableMap.of("customField1", "customValue1"), result.customFields());

    Assertions.assertEquals(2, ((ShellJobTemplate) result).scripts().size());
    List<String> scriptNames =
        ((ShellJobTemplate) result)
            .scripts().stream()
                .map(script -> new File(script).getName())
                .collect(Collectors.toList());
    Assertions.assertTrue(scriptNames.contains(testScript1.getName()));
    Assertions.assertTrue(scriptNames.contains(testScript2.getName()));
  }

  @Test
  public void testCreateSparkRuntimeJobTemplate() throws IOException {
    File executable = Files.createTempFile(tempDir.toPath(), "testSparkJob", ".jar").toFile();
    File jar1 = Files.createTempFile(tempDir.toPath(), "testJar1", ".jar").toFile();
    File jar2 = Files.createTempFile(tempDir.toPath(), "testJar2", ".jar").toFile();

    File file1 = Files.createTempFile(tempDir.toPath(), "testFile1", ".txt").toFile();
    File file2 = Files.createTempFile(tempDir.toPath(), "testFile2", ".txt").toFile();

    File archive1 = Files.createTempFile(tempDir.toPath(), "testArchive1", ".zip").toFile();

    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName("testSparkJob")
            .withComment("This is a test Spark job template")
            .withExecutable(executable.toURI().toString())
            .withClassName("org.apache.gravitino.TestSparkJob")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withJars(Lists.newArrayList(jar1.toURI().toString(), jar2.toURI().toString()))
            .withFiles(Lists.newArrayList(file1.toURI().toString(), file2.toURI().toString()))
            .withArchives(Lists.newArrayList(archive1.toURI().toString()))
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
        JobManager.createRuntimeJobTemplate(
            entity,
            ImmutableMap.of(
                "arg3", "value3",
                "val1", "value1",
                "val2", "value2",
                "customVal1", "customValue1",
                "executor-mem", "4g",
                "driver-cores", "2"),
            tempStagingDir);

    Assertions.assertEquals(sparkJobTemplate.name(), result.name());
    Assertions.assertEquals(sparkJobTemplate.comment(), result.comment());
    Assertions.assertEquals(executable.getName(), new File(result.executable).getName());
    Assertions.assertEquals(Lists.newArrayList("arg1", "arg2", "value3"), result.arguments());
    Assertions.assertEquals(
        ImmutableMap.of("ENV_VAR1", "value1", "ENV_VAR2", "value2"), result.environments());
    Assertions.assertEquals(ImmutableMap.of("customField1", "customValue1"), result.customFields());

    Assertions.assertEquals(2, ((SparkJobTemplate) result).jars().size());
    List<String> jarNames =
        ((SparkJobTemplate) result)
            .jars().stream().map(jar -> new File(jar).getName()).collect(Collectors.toList());
    Assertions.assertTrue(jarNames.contains(jar1.getName()));
    Assertions.assertTrue(jarNames.contains(jar2.getName()));

    Assertions.assertEquals(2, ((SparkJobTemplate) result).files().size());
    List<String> fileNames =
        ((SparkJobTemplate) result)
            .files().stream().map(file -> new File(file).getName()).collect(Collectors.toList());
    Assertions.assertTrue(fileNames.contains(file1.getName()));
    Assertions.assertTrue(fileNames.contains(file2.getName()));

    Assertions.assertEquals(1, ((SparkJobTemplate) result).archives().size());
    List<String> archiveNames =
        ((SparkJobTemplate) result)
            .archives().stream()
                .map(archive -> new File(archive).getName())
                .collect(Collectors.toList());
    Assertions.assertTrue(archiveNames.contains(archive1.getName()));

    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g", "spark.driver.cores", "2"),
        ((SparkJobTemplate) result).configs());
  }
}
