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
package org.apache.gravitino.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationFactory;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies the rolling file appenders of the packaged log4j2 templates: every roll is kept as its
 * own archive, and archives are removed only by age or by the per-log total size cap, and only the
 * archives of the log itself.
 */
class TestLog4j2RollingConfiguration {

  private static final String ROLL_SIZE = "1KB";
  private static final int MESSAGE_COUNT = 400;
  private static final Pattern MESSAGE_ID = Pattern.compile("message-(\\d+)-");

  @TempDir Path logDir;

  static Stream<Arguments> rollingLogs() {
    return Stream.of(
        Arguments.of(
            "conf/log4j2.properties.template",
            "gravitino-server",
            "org.apache.gravitino.TestRootLogger",
            "gravitino-server",
            "serverLogMaxTotalSize"),
        Arguments.of(
            "conf/log4j2.properties.template",
            "gravitino-server",
            "gravitino.audit",
            "gravitino_audit",
            "auditLogMaxTotalSize"),
        Arguments.of(
            "conf/log4j2.properties.template",
            "gravitino-server",
            "org.apache.gravitino.lineage.sink.LineageLogSink$LineageLogger",
            "gravitino_lineage",
            "lineageLogMaxTotalSize"),
        Arguments.of(
            "conf/gravitino-iceberg-rest-log4j2.properties.template",
            "gravitino-iceberg-rest-server",
            "org.apache.gravitino.TestRootLogger",
            "gravitino-iceberg-rest-server",
            "serverLogMaxTotalSize"),
        Arguments.of(
            "conf/gravitino-lance-rest-log4j2.properties.template",
            "gravitino-lance-rest-server",
            "org.apache.gravitino.TestRootLogger",
            "gravitino-lance-rest-server",
            "serverLogMaxTotalSize"));
  }

  @ParameterizedTest(name = "{3}")
  @MethodSource("rollingLogs")
  void testEveryRollIsKeptAndOnlyOwnExpiredArchivesAreDeleted(
      String template, String serverName, String loggerName, String logName, String capProperty)
      throws IOException {
    FileTime expired = FileTime.from(Instant.now().minus(60, ChronoUnit.DAYS));
    Path ownExpiredArchive = createArchive(logDir.resolve(logName + "_20200101.1.log.gz"), expired);
    Path otherExpiredArchive =
        createArchive(logDir.resolve("other-log_20200101.1.log.gz"), expired);
    Path nestedExpiredArchive =
        createArchive(logDir.resolve("nested").resolve(logName + "_20200101.1.log.gz"), expired);

    writeMessages(template, serverName, loggerName, capProperty, "1GB");

    Map<Integer, Path> archives = todayArchives(logName);
    assertTrue(archives.size() > 1, "Expected multiple archives, but found " + archives.keySet());
    assertEquals(
        IntStream.rangeClosed(1, archives.size()).boxed().collect(Collectors.toList()),
        new ArrayList<>(archives.keySet()),
        "Every roll must be kept as its own archive");

    List<Integer> ids = new ArrayList<>();
    for (Path archive : archives.values()) {
      ids.addAll(readMessageIds(archive));
    }
    ids.addAll(readMessageIds(logDir.resolve(logName + ".log")));
    assertEquals(
        IntStream.range(0, MESSAGE_COUNT).boxed().collect(Collectors.toList()),
        ids.stream().sorted().collect(Collectors.toList()),
        "Every message must be kept exactly once");

    assertFalse(Files.exists(ownExpiredArchive), "Expired archive of the log must be deleted");
    assertTrue(Files.exists(otherExpiredArchive), "Archives of other logs must be kept");
    assertTrue(Files.exists(nestedExpiredArchive), "Files in subdirectories must be kept");
  }

  @ParameterizedTest(name = "{3}")
  @MethodSource("rollingLogs")
  void testOldestArchivesAreDeletedBeyondTotalSizeCap(
      String template, String serverName, String loggerName, String logName, String capProperty)
      throws IOException {
    long capBytes = 4096;
    writeMessages(template, serverName, loggerName, capProperty, capBytes / 1024 + "KB");

    TreeMap<Integer, Path> archives = todayArchives(logName);
    assertFalse(archives.isEmpty(), "Expected rolled archives of " + logName);
    long totalSize = 0;
    for (Path archive : archives.values()) {
      totalSize += Files.size(archive);
    }
    assertTrue(totalSize <= capBytes, "Archives use " + totalSize + " bytes beyond the cap");
    assertFalse(archives.containsKey(1), "The oldest archive must be deleted first");
    int newestIndex = archives.lastKey();
    assertTrue(
        newestIndex > archives.size(), "Expected deleted archives, but found " + archives.keySet());
  }

  private void writeMessages(
      String template, String serverName, String loggerName, String capProperty, String cap)
      throws IOException {
    String content =
        new String(Files.readAllBytes(rootDir().resolve(template)), StandardCharsets.UTF_8);
    content = replaceValue(content, "property\\.basePath", logDir.toString());
    content = replaceValue(content, "property\\.serverName", serverName);
    content = replaceValue(content, "property\\." + capProperty, cap);
    content = replaceValue(content, "appender\\.\\w+\\.policies\\.size\\.size", ROLL_SIZE);

    LoggerContext context = new LoggerContext("log4j2-rolling-test-" + UUID.randomUUID());
    try (InputStream input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
      Configuration configuration =
          new PropertiesConfigurationFactory()
              .getConfiguration(context, new ConfigurationSource(input));
      context.start(configuration);
    }

    try {
      for (int i = 0; i < MESSAGE_COUNT; i++) {
        // Random content keeps the gzip archives close to the roll size.
        context
            .getLogger(loggerName)
            .info("message-{}-{}{}", i, UUID.randomUUID(), UUID.randomUUID());
      }
    } finally {
      assertTrue(context.stop(30, TimeUnit.SECONDS), "Log4j2 context did not stop in time");
    }
  }

  private TreeMap<Integer, Path> todayArchives(String logName) throws IOException {
    // Skip the expired archive that a test creates before logging.
    Pattern archiveName =
        Pattern.compile(Pattern.quote(logName) + "_(?!20200101)\\d{8}\\.(\\d+)\\.log\\.gz");
    TreeMap<Integer, Path> archives = new TreeMap<>();
    try (Stream<Path> files = Files.list(logDir)) {
      for (Path file : files.collect(Collectors.toList())) {
        Matcher matcher = archiveName.matcher(file.getFileName().toString());
        if (matcher.matches()) {
          archives.put(Integer.parseInt(matcher.group(1)), file);
        }
      }
    }
    return archives;
  }

  private static String replaceValue(String content, String keyRegex, String value) {
    Matcher matcher = Pattern.compile("(?m)^(" + keyRegex + "\\s*=\\s*).*$").matcher(content);
    assertTrue(matcher.find(), "Missing property " + keyRegex);
    return matcher.replaceAll("$1" + Matcher.quoteReplacement(value));
  }

  private static Path createArchive(Path archive, FileTime lastModified) throws IOException {
    Files.createDirectories(archive.getParent());
    Files.write(archive, new byte[] {0});
    Files.setLastModifiedTime(archive, lastModified);
    return archive;
  }

  private static List<Integer> readMessageIds(Path file) throws IOException {
    List<Integer> ids = new ArrayList<>();
    if (!Files.exists(file)) {
      return ids;
    }

    InputStream input = Files.newInputStream(file);
    if (file.toString().endsWith(".gz")) {
      input = new GZIPInputStream(input);
    }
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        Matcher matcher = MESSAGE_ID.matcher(line);
        if (matcher.find()) {
          ids.add(Integer.parseInt(matcher.group(1)));
        }
      }
    }
    return ids;
  }

  static Path rootDir() {
    String home = System.getenv("GRAVITINO_HOME");
    if (home != null && Files.exists(Paths.get(home, "conf", "log4j2.properties.template"))) {
      return Paths.get(home);
    }
    return Paths.get("").toAbsolutePath().getParent();
  }
}
