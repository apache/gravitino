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

import com.google.common.base.Joiner;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.gravitino.job.HttpJobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProcessBuilder extends LocalProcessBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(HttpProcessBuilder.class);

  protected HttpProcessBuilder(HttpJobTemplate httpJobTemplate, Map<String, String> configs) {
    super(httpJobTemplate, configs);
  }

  @VisibleForTesting
  static List<String> generateHttpCommand(HttpJobTemplate httpJobTemplate) {
    List<String> commandList = new ArrayList<>();
    commandList.add("curl");

    // Add HTTP method (executable)
    commandList.add("-X");
    commandList.add(httpJobTemplate.executable().toUpperCase());

    // Add headers
    for (Map.Entry<String, String> header : httpJobTemplate.headers().entrySet()) {
      commandList.add("-H");
      commandList.add(header.getKey() + ": " + header.getValue());
    }

    // Add body if present
    if (httpJobTemplate.body() != null && !httpJobTemplate.body().isEmpty()) {
      commandList.add("-d");
      commandList.add(httpJobTemplate.body());
    }

    // Add query parameters to URL
    StringBuilder urlBuilder = new StringBuilder(httpJobTemplate.url());
    if (!httpJobTemplate.queryParams().isEmpty()) {
      urlBuilder.append("?");
      urlBuilder.append(String.join("&", httpJobTemplate.queryParams()));
    }
    commandList.add(urlBuilder.toString());

    // Add additional arguments
    commandList.addAll(httpJobTemplate.arguments());

    return commandList;
  }

  @Override
  public Process start() {
    HttpJobTemplate httpJobTemplate = (HttpJobTemplate) jobTemplate;

    // For HTTP jobs, we'll use curl as the executable
    List<String> commandList = generateHttpCommand(httpJobTemplate);

    ProcessBuilder builder = new ProcessBuilder(commandList);
    builder.directory(workingDirectory);
    builder.environment().putAll(httpJobTemplate.environments());

    File outputFile = new File(workingDirectory, "output.log");
    File errorFile = new File(workingDirectory, "error.log");
    builder.redirectOutput(outputFile);
    builder.redirectError(errorFile);

    LOG.info("Starting local HTTP job with command: {}", Joiner.on(" ").join(commandList));

    try {
      return builder.start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start HTTP process", e);
    }
  }

  /**
   * Creates an HttpProcessBuilder instance.
   *
   * @param httpJobTemplate the HTTP job template
   * @param configs the configuration map
   * @return an HttpProcessBuilder instance
   */
  public static HttpProcessBuilder create(
      HttpJobTemplate httpJobTemplate, Map<String, String> configs) {
    return new HttpProcessBuilder(httpJobTemplate, configs);
  }
}
