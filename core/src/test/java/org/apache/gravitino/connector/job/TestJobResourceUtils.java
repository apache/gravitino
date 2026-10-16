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
package org.apache.gravitino.connector.job;

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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.utils.FileFetcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobResourceUtils {

  private static File tempDir;
  private File tempStagingDir;

  @BeforeAll
  public static void setUpClass() throws IOException {
    tempDir = Files.createTempDirectory("job-resource-test").toFile();
  }

  @AfterAll
  public static void tearDownClass() throws IOException {
    if (tempDir != null && tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir);
      tempDir = null;
    }
  }

  @BeforeEach
  public void setUp() throws IOException {
    tempStagingDir = Files.createTempDirectory(tempDir.toPath(), "staging").toFile();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (tempStagingDir != null && tempStagingDir.exists()) {
      FileUtils.deleteDirectory(tempStagingDir);
      tempStagingDir = null;
    }
  }

  @Test
  public void testLocalizeShellJobTemplate() throws IOException {
    File executable = Files.createTempFile(tempDir.toPath(), "run", ".sh").toFile();
    File script = Files.createTempFile(tempDir.toPath(), "lib", ".sh").toFile();
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withComment("comment")
            .withExecutable(executable.toURI().toString())
            .withArguments(Lists.newArrayList("--date", "2026-09-28"))
            .withEnvironments(ImmutableMap.of("KEY", "VALUE"))
            .withCustomFields(ImmutableMap.of("field", "value"))
            .withScripts(Lists.newArrayList(script.toURI().toString()))
            .build();

    ShellJobTemplate result =
        (ShellJobTemplate) JobResourceUtils.localizeJobTemplate(template, tempStagingDir);

    Assertions.assertEquals(
        new File(tempStagingDir, executable.getName()).getAbsolutePath(), result.executable());
    Assertions.assertEquals(
        Lists.newArrayList(new File(tempStagingDir, script.getName()).getAbsolutePath()),
        result.scripts());
    Assertions.assertEquals(template.name(), result.name());
    Assertions.assertEquals(template.comment(), result.comment());
    Assertions.assertEquals(template.arguments(), result.arguments());
    Assertions.assertEquals(template.environments(), result.environments());
    Assertions.assertEquals(template.customFields(), result.customFields());
  }

  @Test
  public void testLocalizeJobTemplateTwiceIntoSameDir() throws IOException {
    File executable = Files.createTempFile(tempDir.toPath(), "run", ".sh").toFile();
    Files.writeString(executable.toPath(), "echo hi");
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable(executable.toURI().toString())
            .build();

    // A job executor may localize a job template that was already localized into the same
    // directory, for example a subclass calling its parent's submitJob.
    JobTemplate localized = JobResourceUtils.localizeJobTemplate(template, tempStagingDir);
    JobTemplate localizedAgain = JobResourceUtils.localizeJobTemplate(localized, tempStagingDir);

    Assertions.assertEquals(localized.executable(), localizedAgain.executable());
    Assertions.assertEquals(
        "echo hi", Files.readString(new File(localizedAgain.executable()).toPath()));
  }

  @Test
  public void testIsCommandName() {
    for (String command : List.of("python", "bash", "python3.11", "spark-submit", "run.sh")) {
      Assertions.assertTrue(JobResourceUtils.isCommandName(command), command);
    }
    for (String notCommand :
        Arrays.asList(
            null,
            "",
            " ",
            ".",
            "..",
            "/bin/bash",
            "./run.sh",
            "jobs/run.sh",
            "file:///bin/bash",
            "https://repo.example.com/run.sh",
            "hdfs://nn/run.sh",
            "my command")) {
      Assertions.assertFalse(JobResourceUtils.isCommandName(notCommand), notCommand);
    }
  }

  @Test
  public void testLocalizeShellJobTemplateKeepsCommandName() throws IOException {
    File script = Files.createTempFile(tempDir.toPath(), "job", ".py").toFile();
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("python_job")
            .withExecutable("python")
            .withArguments(Lists.newArrayList(script.getName()))
            .withScripts(Lists.newArrayList(script.toURI().toString()))
            .build();

    ShellJobTemplate result =
        (ShellJobTemplate) JobResourceUtils.localizeJobTemplate(template, tempStagingDir);

    // The command is run from the PATH, only the script is fetched.
    Assertions.assertEquals("python", result.executable());
    Assertions.assertEquals(
        Lists.newArrayList(new File(tempStagingDir, script.getName()).getAbsolutePath()),
        result.scripts());
    Assertions.assertArrayEquals(new String[] {script.getName()}, tempStagingDir.list());
  }

  @Test
  public void testLocalizeRejectsResourcesWithSameFileName() throws IOException {
    File dirA = Files.createTempDirectory(tempDir.toPath(), "a").toFile();
    File dirB = Files.createTempDirectory(tempDir.toPath(), "b").toFile();
    File executable = new File(dirA, "run.sh");
    File script = new File(dirB, "run.sh");
    Assertions.assertTrue(executable.createNewFile());
    Assertions.assertTrue(script.createNewFile());

    // Different resources with the same file name would overwrite each other.
    ShellJobTemplate shellTemplate =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable(executable.getAbsolutePath())
            .withScripts(Lists.newArrayList(script.getAbsolutePath()))
            .build();
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JobResourceUtils.localizeJobTemplate(shellTemplate, tempStagingDir));
    Assertions.assertTrue(e.getMessage().contains("same file name run.sh"), e.getMessage());

    SparkJobTemplate sparkTemplate =
        SparkJobTemplate.builder()
            .withName("spark_job")
            .withExecutable("https://a.example.com/jobs/app.jar")
            .withClassName("org.example.App")
            .withFiles(Lists.newArrayList("https://b.example.com/conf/app.jar"))
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JobResourceUtils.localizeJobTemplate(sparkTemplate, tempStagingDir));

    // Nothing is fetched when the job template is rejected.
    Assertions.assertArrayEquals(new String[0], tempStagingDir.list());

    // The same resource listed twice is not a conflict.
    ShellJobTemplate repeated =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable(executable.getAbsolutePath())
            .withScripts(Lists.newArrayList(executable.getAbsolutePath()))
            .build();
    Assertions.assertDoesNotThrow(
        () -> JobResourceUtils.localizeJobTemplate(repeated, tempStagingDir));
  }

  @Test
  public void testLocalizeRejectsScriptNamedLikeCommandNameExecutable() throws IOException {
    File script = new File(Files.createTempDirectory(tempDir.toPath(), "src").toFile(), "run.sh");
    Assertions.assertTrue(script.createNewFile());
    // "run.sh" is a command name, so the fetched run.sh would not be the one that runs.
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable("run.sh")
            .withScripts(Lists.newArrayList(script.toURI().toString()))
            .build();

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> JobResourceUtils.localizeJobTemplate(template, tempStagingDir));
    Assertions.assertTrue(e.getMessage().contains("command name"), e.getMessage());
    Assertions.assertArrayEquals(new String[0], tempStagingDir.list());
  }

  @Test
  public void testFetchFileRejectsUriWithoutFileName() {
    for (String uri :
        Arrays.asList("http://repo.example.com", "http://repo.example.com/", "file:relative")) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> JobResourceUtils.fetchFile(uri, tempStagingDir, 1000),
              uri);
      Assertions.assertTrue(e.getMessage().contains("no file name"), e.getMessage());
    }
    // The staging directory itself is never the fetch destination.
    Assertions.assertTrue(tempStagingDir.isDirectory());
    Assertions.assertArrayEquals(new String[0], tempStagingDir.list());
  }

  @Test
  public void testLocalizeSparkJobTemplate() throws IOException {
    File executable = Files.createTempFile(tempDir.toPath(), "app", ".jar").toFile();
    File jar = Files.createTempFile(tempDir.toPath(), "lib", ".jar").toFile();
    File file = Files.createTempFile(tempDir.toPath(), "conf", ".properties").toFile();
    File archive = Files.createTempFile(tempDir.toPath(), "deps", ".zip").toFile();
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("spark_job")
            .withExecutable(executable.toURI().toString())
            .withClassName("org.example.App")
            .withArguments(Lists.newArrayList("arg"))
            .withJars(Lists.newArrayList(jar.toURI().toString()))
            .withFiles(Lists.newArrayList(file.toURI().toString()))
            .withArchives(Lists.newArrayList(archive.toURI().toString()))
            .withConfigs(ImmutableMap.of("spark.executor.instances", "2"))
            .build();

    SparkJobTemplate result =
        (SparkJobTemplate) JobResourceUtils.localizeJobTemplate(template, tempStagingDir);

    Assertions.assertEquals(
        new File(tempStagingDir, executable.getName()).getAbsolutePath(), result.executable());
    Assertions.assertEquals(
        Lists.newArrayList(new File(tempStagingDir, jar.getName()).getAbsolutePath()),
        result.jars());
    Assertions.assertEquals(
        Lists.newArrayList(new File(tempStagingDir, file.getName()).getAbsolutePath()),
        result.files());
    Assertions.assertEquals(
        Lists.newArrayList(new File(tempStagingDir, archive.getName()).getAbsolutePath()),
        result.archives());
    Assertions.assertEquals(template.className(), result.className());
    Assertions.assertEquals(template.arguments(), result.arguments());
    Assertions.assertEquals(template.configs(), result.configs());
  }

  @Test
  public void testFetchFile() throws IOException {
    File testFile1 = Files.createTempFile(tempDir.toPath(), "testFile1", ".txt").toFile();
    String result =
        JobResourceUtils.fetchFile(testFile1.toURI().toString(), tempStagingDir, 30 * 1000);
    File resultFile = new File(result);
    Assertions.assertEquals(testFile1.getName(), resultFile.getName());

    File testFile2 = Files.createTempFile(tempDir.toPath(), "testFile2", ".txt").toFile();
    File testFile3 = Files.createTempFile(tempDir.toPath(), "testFile3", ".txt").toFile();

    List<String> expectedUris =
        Lists.newArrayList(testFile2.toURI().toString(), testFile3.toURI().toString());
    List<String> resultUris = JobResourceUtils.fetchFiles(expectedUris, tempStagingDir, 30 * 1000);

    Assertions.assertEquals(2, resultUris.size());
    List<String> resultFileNames =
        resultUris.stream().map(uri -> new File(uri).getName()).collect(Collectors.toList());
    Assertions.assertTrue(resultFileNames.contains(testFile2.getName()));
    Assertions.assertTrue(resultFileNames.contains(testFile3.getName()));
  }

  @Test
  public void testFetchFileWithMissingLocalFileShouldFail() throws IOException {
    File stagingDir = tempStagingDir;

    Path missingFilePath =
        Path.of(System.getProperty("java.io.tmpdir"), "missing-job-file-" + UUID.randomUUID());
    String uri = missingFilePath.toUri().toString();

    Assertions.assertThrows(
        RuntimeException.class, () -> JobResourceUtils.fetchFile(uri, stagingDir, 1000));
  }

  @Test
  public void testFetchFileSsrfBlocked() {
    File stagingDir = tempStagingDir;
    FileFetcher.get().initialize(true);

    // Loopback address
    RuntimeException e1 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobResourceUtils.fetchFile("http://127.0.0.1:8090/configs", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e1);

    // AWS / GCP / Azure cloud-metadata endpoint (link-local 169.254.x.x)
    RuntimeException e2 =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                JobResourceUtils.fetchFile(
                    "http://169.254.169.254/latest/meta-data/", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e2);

    // RFC-1918 private range
    RuntimeException e3 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobResourceUtils.fetchFile("http://192.168.1.1/run.sh", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e3);

    // Alibaba Cloud / Oracle Cloud metadata endpoint
    RuntimeException e4 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> JobResourceUtils.fetchFile("http://100.100.100.200/run.sh", stagingDir, 1000));
    assertRemoteUriBlockedMessage(e4);
  }

  @Test
  public void testFetchFileShouldAllowLocalhostWhenBlockingDisabled() throws Exception {
    File stagingDir = tempStagingDir;
    HttpServer server = createLoopbackHttpServer("job artifact");

    try {
      server.start();
      int port = server.getAddress().getPort();
      FileFetcher.get().initialize(false);

      String fetchedFile =
          JobResourceUtils.fetchFile(
              String.format("http://127.0.0.1:%d/artifact.jar", port), stagingDir, 1000);

      Assertions.assertEquals("job artifact", Files.readString(Path.of(fetchedFile)));
    } finally {
      FileFetcher.get().initialize(true);
      server.stop(0);
    }
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

  private static void assertRemoteUriBlockedMessage(RuntimeException exception) {
    Assertions.assertTrue(exception.getCause().getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(
        exception.getCause().getMessage().contains(FileFetcher.BLOCK_UNSAFE_REMOTE_URI_CONFIG));
  }
}
