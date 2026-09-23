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
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.FileFetcher;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobTemplateResolver {

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
  public void testFetchFilesFromUir() throws IOException {
    File testFile1 = Files.createTempFile(tempDir.toPath(), "testFile1", ".txt").toFile();
    String result =
        JobTemplateResolver.fetchFileFromUri(
            testFile1.toURI().toString(), tempStagingDir, 30 * 1000);
    File resultFile = new File(result);
    Assertions.assertEquals(testFile1.getName(), resultFile.getName());

    File testFile2 = Files.createTempFile(tempDir.toPath(), "testFile2", ".txt").toFile();
    File testFile3 = Files.createTempFile(tempDir.toPath(), "testFile3", ".txt").toFile();

    List<String> expectedUris =
        Lists.newArrayList(testFile2.toURI().toString(), testFile3.toURI().toString());
    List<String> resultUris =
        JobTemplateResolver.fetchFilesFromUri(expectedUris, tempStagingDir, 30 * 1000);

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
        new JobTemplateResolver(entity)
            .resolve(
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
  public void testCreateShellRuntimeJobTemplateWithReplacementsInScripts() throws IOException {
    File testScript1 = Files.createTempFile(tempDir.toPath(), "testScript1", ".sh").toFile();
    File testScript2 = Files.createTempFile(tempDir.toPath(), "testScript2", ".sh").toFile();

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
                    testScript1.toURI().toString().replace("testScript1", "{{scriptName1}}"),
                    testScript2.toURI().toString().replace("testScript2", "{{scriptName2}}")))
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
                    "scriptName2", "testScript2"),
                tempStagingDir);

    Assertions.assertEquals("echo", new File(result.executable).getName());
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
        new JobTemplateResolver(entity)
            .resolve(
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

  @Test
  public void testCreateSparkRuntimeJobTemplateWithReplacements() throws IOException {
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
            .withExecutable(executable.toURI().toString().replace("test", "{{env}}"))
            .withClassName("org.apache.gravitino.TestSparkJob")
            .withArguments(Lists.newArrayList("arg1", "arg2", "{{arg3}}"))
            .withEnvironments(ImmutableMap.of("ENV_VAR1", "{{val1}}", "ENV_VAR2", "{{val2}}"))
            .withCustomFields(ImmutableMap.of("customField1", "{{customVal1}}"))
            .withJars(
                Lists.newArrayList(
                    jar1.toURI().toString().replace("test", "{{env}}"),
                    jar2.toURI().toString().replace("test", "{{env}}")))
            .withFiles(
                Lists.newArrayList(
                    file1.toURI().toString().replace("test", "{{env}}"),
                    file2.toURI().toString().replace("test", "{{env}}")))
            .withArchives(
                Lists.newArrayList(archive1.toURI().toString().replace("test", "{{env}}")))
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
                    "env", "test"),
                tempStagingDir);

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

    JobTemplate result =
        new JobTemplateResolver(entity).resolve(ImmutableMap.of("table", "t"), tempStagingDir);
    Assertions.assertEquals(
        Lists.newArrayList("--table", "t", "--mode", "full", ""), result.arguments());
  }

  @Test
  public void testCreateFailsBeforeFetchingOnMissingParameters() {
    JobTemplateEntity entity = shellTemplateEntity(Lists.newArrayList("{{table}}"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new JobTemplateResolver(entity).resolve(ImmutableMap.of(), tempStagingDir));
    // The executable is not fetched when a parameter is missing.
    String[] staged = tempStagingDir.list();
    Assertions.assertTrue(staged == null || staged.length == 0);
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
                new JobTemplateResolver(entity)
                    .resolve(ImmutableMap.of("a", "SAME", "b", "SAME"), tempStagingDir));
    Assertions.assertTrue(e.getMessage().contains("SAME"), e.getMessage());
  }

  private static HttpServer createLoopbackHttpServer(String response) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/artifact.jar",
        exchange -> {
          byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
          }
        });
    return server;
  }

  @Test
  public void testFetchFileFromUriWithMissingLocalFileShouldFail() throws IOException {
    File stagingDir = tempStagingDir;

    Path missingFilePath =
        Path.of(System.getProperty("java.io.tmpdir"), "missing-job-file-" + UUID.randomUUID());
    String uri = missingFilePath.toUri().toString();

    Assertions.assertThrows(
        RuntimeException.class, () -> JobTemplateResolver.fetchFileFromUri(uri, stagingDir, 1000));
  }

  @Test
  public void testFetchFileFromUriSsrfBlocked() {
    File stagingDir = tempStagingDir;
    FileFetcher.get().initialize(true);

    // Loopback address
    RuntimeException e1 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JobTemplateResolver.fetchFileFromUri(
                    "http://127.0.0.1:8090/configs", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e1);

    // AWS / GCP / Azure cloud-metadata endpoint (link-local 169.254.x.x)
    RuntimeException e2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JobTemplateResolver.fetchFileFromUri(
                    "http://169.254.169.254/latest/meta-data/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e2);

    // RFC-1918 private range
    RuntimeException e3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobTemplateResolver.fetchFileFromUri("http://192.168.1.1/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e3);

    // Alibaba Cloud / Oracle Cloud metadata endpoint
    RuntimeException e4 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JobTemplateResolver.fetchFileFromUri("http://100.100.100.200/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e4);
  }

  @Test
  public void testFetchFileFromUriShouldAllowLocalhostWhenBlockingDisabled() throws Exception {
    File stagingDir = tempStagingDir;
    HttpServer server = createLoopbackHttpServer("job artifact");

    try {
      server.start();
      int port = server.getAddress().getPort();
      FileFetcher.get().initialize(false);

      String fetchedFile =
          JobTemplateResolver.fetchFileFromUri(
              String.format("http://127.0.0.1:%d/artifact.jar", port), stagingDir, 1000);

      Assertions.assertEquals("job artifact", Files.readString(Path.of(fetchedFile)));
    } finally {
      FileFetcher.get().initialize(true);
      server.stop(0);
    }
  }

  private static void assertRemoteUriBlockedMessage(RuntimeException exception) {
    Assertions.assertTrue(exception.getCause().getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(
        exception.getCause().getMessage().contains(FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG));
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
