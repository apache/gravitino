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
package org.apache.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Functional tests for gradle/build-info.gradle.kts, the script that writes the git commit id into
 * gravitino-build-info.properties. Each test applies the real script to a tiny Gradle project and
 * checks the generated resource and the single build-info entry in the resulting JAR.
 */
public class TestBuildInfoGeneration {

  private static final String BUILD_INFO = "gravitino-build-info.properties";
  private static final String NO_GIT = "-PgitExecutable=gravitino-no-such-git";

  @TempDir Path tempDir;

  @BeforeAll
  public static void checkGit() {
    assumeTrue(runGit(Paths.get("").toAbsolutePath(), "--version") != null, "git is required");
  }

  @Test
  public void testLooseRefsUpToDateAndHeadChange() throws Exception {
    Path project = newRepo("project");

    BuildResult first = build(project);
    assertEquals(TaskOutcome.SUCCESS, first.task(":generateBuildInfo").getOutcome());
    assertCommitId(project, head(project));

    BuildResult second = build(project);
    assertEquals(TaskOutcome.UP_TO_DATE, second.task(":generateBuildInfo").getOutcome());
    assertEquals(TaskOutcome.UP_TO_DATE, second.task(":processResources").getOutcome());
    assertEquals(TaskOutcome.UP_TO_DATE, second.task(":jar").getOutcome());

    runGit(project, "commit", "--allow-empty", "-m", "next");
    BuildResult third = build(project);
    assertEquals(TaskOutcome.SUCCESS, third.task(":generateBuildInfo").getOutcome());
    assertEquals(TaskOutcome.SUCCESS, third.task(":jar").getOutcome());
    assertCommitId(project, head(project));
  }

  @Test
  public void testPackedRefsWithAndWithoutGit() throws Exception {
    Path project = newRepo("project");
    runGit(project, "pack-refs", "--all");

    build(project);
    assertCommitId(project, head(project));

    deleteBuildOutput(project);
    build(project, NO_GIT);
    assertCommitId(project, head(project));
  }

  @Test
  public void testLinkedWorktrees() throws Exception {
    Path main = newRepo("main");
    Path named = tempDir.resolve("named");
    Path detached = tempDir.resolve("detached");
    runGit(main, "worktree", "add", "-b", "wt", named.toString());
    runGit(main, "worktree", "add", "--detach", detached.toString());
    runGit(main, "pack-refs", "--all");

    for (Path worktree : List.of(named, detached)) {
      build(worktree);
      assertCommitId(worktree, head(worktree));

      deleteBuildOutput(worktree);
      build(worktree, NO_GIT);
      assertCommitId(worktree, head(worktree));
    }
  }

  @Test
  public void testStaleSourceCopyDoesNotShadowGeneratedFile() throws Exception {
    Path project = newRepo("project");
    Path stale = project.resolve("src/main/resources").resolve(BUILD_INFO);
    Files.createDirectories(stale.getParent());
    Files.writeString(stale, "git.commit.id=stale\n");

    build(project);
    assertCommitId(project, head(project));
  }

  @Test
  public void testNoGitDirectoryGivesEmptyCommitId() throws Exception {
    Path project = tempDir.resolve("tarball");
    writeProject(project);

    build(project);
    assertCommitId(project, "");
  }

  private Path newRepo(String name) throws IOException {
    Path project = tempDir.resolve(name);
    writeProject(project);
    Files.writeString(project.resolve(".gitignore"), "build/\n.gradle/\n");
    runGit(project, "init", "-q", "-b", "main");
    runGit(project, "add", ".");
    runGit(project, "commit", "-q", "-m", "init");
    return project;
  }

  private static void writeProject(Path project) throws IOException {
    Files.createDirectories(project);
    Files.writeString(project.resolve("settings.gradle.kts"), "rootProject.name = \"info-test\"\n");
    Files.writeString(
        project.resolve("build.gradle.kts"),
        "plugins { java }\n" + "version = \"1.0\"\n" + "apply(from = \"build-info.gradle.kts\")\n");
    Files.copy(
        Paths.get(System.getProperty("buildInfoScript")), project.resolve("build-info.gradle.kts"));
  }

  private static BuildResult build(Path project, String... extraArgs) {
    List<String> args = new ArrayList<>(List.of("jar", "--stacktrace"));
    Collections.addAll(args, extraArgs);
    return GradleRunner.create().withProjectDir(project.toFile()).withArguments(args).build();
  }

  private static void deleteBuildOutput(Path project) throws IOException {
    try (var paths = Files.walk(project.resolve("build"))) {
      paths.sorted(Collections.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }

  /** Asserts that the generated resource and the only build-info entry in the JAR match. */
  private static void assertCommitId(Path project, String expected) throws IOException {
    Properties generated = new Properties();
    try (InputStream in =
        Files.newInputStream(project.resolve("build/generated/build-info/" + BUILD_INFO))) {
      generated.load(in);
    }
    assertEquals(expected, generated.getProperty("git.commit.id"));

    try (ZipFile jar = new ZipFile(project.resolve("build/libs/info-test-1.0.jar").toFile())) {
      int entries = 0;
      Properties packaged = new Properties();
      Enumeration<? extends ZipEntry> all = jar.entries();
      while (all.hasMoreElements()) {
        ZipEntry entry = all.nextElement();
        if (entry.getName().equals(BUILD_INFO)) {
          entries++;
          try (InputStream in = jar.getInputStream(entry)) {
            packaged.load(in);
          }
        }
      }
      assertEquals(1, entries);
      assertEquals(expected, packaged.getProperty("git.commit.id"));
    }
  }

  private static String head(Path dir) {
    return runGit(dir, "rev-parse", "HEAD");
  }

  private static String runGit(Path dir, String... args) {
    List<String> command =
        new ArrayList<>(
            List.of(
                "git",
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false"));
    Collections.addAll(command, args);
    try {
      Process process =
          new ProcessBuilder(command)
              .directory(dir.toFile())
              .redirectError(ProcessBuilder.Redirect.DISCARD)
              .start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
        return null;
      }
      return output.trim();
    } catch (IOException | InterruptedException e) {
      return null;
    }
  }
}
