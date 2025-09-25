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

import static org.apache.gravitino.job.local.LocalJobExecutorConfigs.SPARK_HOME;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.job.SparkJobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkProcessBuilder class is responsible for constructing and starting a local Spark job
 * process using the spark-submit command. Using local job executor to run Spark job has some
 * limitations: 1. It uses the aliveness of the spark-submit process to determine if the job is
 * still running, which is not accurate for the cluster deploy mode. 2. It cannot support user
 * impersonation for now.
 */
public class SparkProcessBuilder extends LocalProcessBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProcessBuilder.class);

  private static final String ENV_SPARK_HOME = "SPARK_HOME";

  private final String sparkSubmit;

  protected SparkProcessBuilder(SparkJobTemplate sparkJobTemplate, Map<String, String> configs) {
    super(sparkJobTemplate, configs);
    String sparkHome =
        Optional.ofNullable(configs.get(SPARK_HOME)).orElse(System.getenv(ENV_SPARK_HOME));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sparkHome),
        "gravitino.jobExecutor.local.sparkHome or SPARK_HOME environment variable must"
            + " be set for Spark jobs");

    this.sparkSubmit = sparkHome + "/bin/spark-submit";
    File sparkSubmitFile = new File(sparkSubmit);
    Preconditions.checkArgument(
        sparkSubmitFile.canExecute(),
        "spark-submit is not found or not executable: " + sparkSubmit);
  }

  @VisibleForTesting
  static List<String> generateSparkSubmitCommand(
      String sparkSubmit, SparkJobTemplate sparkJobTemplate) {
    List<String> commandList = Lists.newArrayList(sparkSubmit);

    // Add the main class
    if (StringUtils.isNotBlank(sparkJobTemplate.className())) {
      commandList.add("--class");
      commandList.add(sparkJobTemplate.className());
    }

    // Add jars if it is not empty
    if (!sparkJobTemplate.jars().isEmpty()) {
      commandList.add("--jars");
      commandList.add(String.join(",", sparkJobTemplate.jars()));
    }

    // Add files if it is not empty
    if (!sparkJobTemplate.files().isEmpty()) {
      commandList.add("--files");
      commandList.add(String.join(",", sparkJobTemplate.files()));
    }

    // Add archives if it is not empty
    if (!sparkJobTemplate.archives().isEmpty()) {
      commandList.add("--archives");
      commandList.add(String.join(",", sparkJobTemplate.archives()));
    }

    // Add the Spark configs
    for (Map.Entry<String, String> entry : sparkJobTemplate.configs().entrySet()) {
      commandList.add("--conf");
      commandList.add(entry.getKey() + "=" + entry.getValue());
    }

    // Add the main executable
    commandList.add(sparkJobTemplate.executable());

    // Add the arguments
    commandList.addAll(sparkJobTemplate.arguments());

    return commandList;
  }

  @Override
  public Process start() {
    SparkJobTemplate sparkJobTemplate = (SparkJobTemplate) jobTemplate;
    List<String> commandList = generateSparkSubmitCommand(sparkSubmit, sparkJobTemplate);

    ProcessBuilder builder = new ProcessBuilder(commandList);
    builder.directory(workingDirectory);
    builder.environment().putAll(sparkJobTemplate.environments());

    File outputFile = new File(workingDirectory, "output.log");
    File errorFile = new File(workingDirectory, "error.log");

    builder.redirectOutput(outputFile);
    builder.redirectError(errorFile);

    LOG.info(
        "Starting local Spark job with command: {}, environment variables: {}",
        Joiner.on(" ").join(commandList),
        Joiner.on(", ").withKeyValueSeparator(": ").join(sparkJobTemplate.environments()));

    try {
      return builder.start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start Spark process", e);
    }
  }
}
