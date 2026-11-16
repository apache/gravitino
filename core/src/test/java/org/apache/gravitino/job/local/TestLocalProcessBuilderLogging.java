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

package org.apache.gravitino.job.local;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TestLocalProcessBuilderLogging {

  private static final String UPDATER_OPTIONS =
      "{\"gravitino_uri\":\"http://localhost:8090\",\"password\":\"updater-pw\"}";

  private static final String SPARK_CONF =
      "{\"spark.sql.catalog.ice.rest.auth.basic.password\":\"catalog-pw\"}";

  private File workingDir;

  @BeforeEach
  public void setUp() throws IOException {
    workingDir = Files.createTempDirectory("gravitino-test-process-builder-logging").toFile();
  }

  @AfterEach
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(workingDir);
  }

  @Test
  public void testShellJobStartLogMasksCredentials() throws Throwable {
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("shell-job")
            .withExecutable("echo")
            .withArguments(
                Lists.newArrayList(
                    "--updater-options", UPDATER_OPTIONS, "--spark-conf=" + SPARK_CONF))
            .build();
    ShellProcessBuilder builder =
        new ShellProcessBuilder(template, workingDir, Collections.emptyMap());

    List<String> messages = captureInfo(ShellProcessBuilder.class, () -> waitFor(builder.start()));

    assertMasked(messages);
    assertProcessGotPlaintext();
  }

  @Test
  public void testSparkJobStartLogMasksCredentials() throws Throwable {
    File sparkHome = new File(workingDir, "spark-home");
    File sparkSubmit = new File(sparkHome, "bin/spark-submit");
    FileUtils.writeStringToFile(sparkSubmit, "#!/bin/sh\necho \"$@\"\n", StandardCharsets.UTF_8);
    Assertions.assertTrue(sparkSubmit.setExecutable(true));
    SparkJobTemplate template =
        SparkJobTemplate.builder()
            .withName("spark-job")
            .withExecutable("/path/to/app.jar")
            .withClassName("org.example.App")
            .withArguments(
                Lists.newArrayList(
                    "--updater-options", UPDATER_OPTIONS, "--spark-conf", SPARK_CONF))
            .build();
    SparkProcessBuilder builder =
        new SparkProcessBuilder(
            template,
            workingDir,
            ImmutableMap.of(LocalJobExecutorConfigs.SPARK_HOME, sparkHome.getAbsolutePath()));

    List<String> messages = captureInfo(SparkProcessBuilder.class, () -> waitFor(builder.start()));

    assertMasked(messages);
    assertProcessGotPlaintext();
  }

  private void assertMasked(List<String> messages) {
    Assertions.assertEquals(1, messages.size(), messages.toString());
    String message = messages.get(0);
    Assertions.assertFalse(message.contains("updater-pw"), message);
    Assertions.assertFalse(message.contains("catalog-pw"), message);
    Assertions.assertTrue(message.contains(HiddenPropertyMaskUtils.MASKED_VALUE), message);
    Assertions.assertTrue(message.contains("http://localhost:8090"), message);
  }

  private void assertProcessGotPlaintext() throws IOException {
    String output =
        FileUtils.readFileToString(
            new File(workingDir, LocalProcessBuilder.STDOUT_FILE_NAME), StandardCharsets.UTF_8);
    Assertions.assertTrue(output.contains("updater-pw"), output);
    Assertions.assertTrue(output.contains("catalog-pw"), output);
  }

  private static void waitFor(Process process) throws InterruptedException {
    Assertions.assertTrue(process.waitFor(1, TimeUnit.MINUTES));
    Assertions.assertEquals(0, process.exitValue());
  }

  private static List<String> captureInfo(Class<?> loggerClass, Executable action)
      throws Throwable {
    LoggerContext context =
        (LoggerContext) LogManager.getContext(loggerClass.getClassLoader(), false);
    AbstractConfiguration configuration = (AbstractConfiguration) context.getConfiguration();
    MessageCollector collector = new MessageCollector();
    collector.start();
    configuration.addAppender(collector);
    LoggerConfig loggerConfig = new LoggerConfig(loggerClass.getName(), Level.INFO, false);
    loggerConfig.addAppender(collector, Level.INFO, null);
    configuration.addLogger(loggerClass.getName(), loggerConfig);
    context.updateLoggers();
    try {
      action.execute();
    } finally {
      configuration.removeLogger(loggerClass.getName());
      collector.stop();
      configuration.removeAppender(collector.getName());
      context.updateLoggers();
    }
    return ImmutableList.copyOf(collector.messages);
  }

  private static class MessageCollector extends AbstractAppender {
    private final List<String> messages = Collections.synchronizedList(Lists.newArrayList());

    MessageCollector() {
      super("processBuilderMessages", null, PatternLayout.createDefaultLayout(), true, null);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }
  }
}
