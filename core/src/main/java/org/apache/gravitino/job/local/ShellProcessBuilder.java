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

import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.ShellJobTemplate;

public class ShellProcessBuilder extends LocalProcessBuilder {

  protected ShellProcessBuilder(ShellJobTemplate shellJobTemplate, Map<String, String> configs) {
    super(shellJobTemplate, configs);
  }

  @Override
  public Process start() {
    ShellJobTemplate shellJobTemplate = (ShellJobTemplate) jobTemplate;
    File executableFile = new File(shellJobTemplate.executable());
    if (!executableFile.canExecute()) {
      executableFile.setExecutable(true);
    }

    List<String> commandList = Lists.newArrayList(shellJobTemplate.executable());
    commandList.addAll(shellJobTemplate.arguments());

    ProcessBuilder builder = new ProcessBuilder(commandList);
    builder.directory(workingDirectory);
    builder.environment().putAll(shellJobTemplate.environments());

    File outputFile = new File(workingDirectory, "output.log");
    File errorFile = new File(workingDirectory, "error.log");
    builder.redirectOutput(outputFile);
    builder.redirectError(errorFile);

    try {
      return builder.start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start shell process", e);
    }
  }
}
