/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.integration.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// zeppelin-integration/src/test/java/org/apache/zeppelin/CommandExecutor.java
public class CommandExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

  public enum IGNORE_ERRORS {
    TRUE,
    FALSE
  }

  public static int NORMAL_EXIT = 0;

  private static IGNORE_ERRORS DEFAULT_BEHAVIOUR_ON_ERRORS = IGNORE_ERRORS.TRUE;

  public static Object executeCommandLocalHost(
      String[] command,
      boolean printToConsole,
      ProcessData.TypesOfData type,
      IGNORE_ERRORS ignore_errors,
      Map<String, String> env) {
    List<String> subCommandsAsList = new ArrayList<>(Arrays.asList(command));
    String mergedCommand = StringUtils.join(subCommandsAsList, " ");

    LOG.info("Sending command \"{}\" to localhost", mergedCommand);

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    processBuilder.environment().putAll(env);
    Process process = null;
    try {
      process = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ProcessData dataOfProcess = new ProcessData(process, printToConsole);
    Object outputOfProcess = dataOfProcess.getData(type);
    int exit_code = dataOfProcess.getExitCodeValue();

    if (!printToConsole) LOG.trace(outputOfProcess.toString());
    else LOG.debug(outputOfProcess.toString());
    if (ignore_errors == IGNORE_ERRORS.FALSE && exit_code != NORMAL_EXIT) {
      LOG.error(String.format("Command '%s' failed with exit code %s", mergedCommand, exit_code));
    }
    return outputOfProcess;
  }

  public static Object executeCommandLocalHost(
      String command, boolean printToConsole, ProcessData.TypesOfData type) {
    return executeCommandLocalHost(
        new String[] {"bash", "-c", command},
        printToConsole,
        type,
        DEFAULT_BEHAVIOUR_ON_ERRORS,
        new HashMap<String, String>());
  }

  public static Object executeCommandLocalHost(
      String command,
      boolean printToConsole,
      ProcessData.TypesOfData type,
      Map<String, String> env) {
    return executeCommandLocalHost(
        new String[] {"bash", "-c", command},
        printToConsole,
        type,
        DEFAULT_BEHAVIOUR_ON_ERRORS,
        env);
  }
}
