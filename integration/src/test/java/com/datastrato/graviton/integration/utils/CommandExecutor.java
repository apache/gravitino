/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      ProcessData.Types_Of_Data type,
      IGNORE_ERRORS ignore_errors) {
    List<String> subCommandsAsList = new ArrayList<>(Arrays.asList(command));
    String mergedCommand = StringUtils.join(subCommandsAsList, " ");

    LOG.info("Sending command \"" + mergedCommand + "\" to localhost");

    ProcessBuilder processBuilder = new ProcessBuilder(command);
    Process process = null;
    try {
      process = processBuilder.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ProcessData data_of_process = new ProcessData(process, printToConsole);
    Object output_of_process = data_of_process.getData(type);
    int exit_code = data_of_process.getExitCodeValue();

    if (!printToConsole) LOG.trace(output_of_process.toString());
    else LOG.debug(output_of_process.toString());
    if (ignore_errors == IGNORE_ERRORS.FALSE && exit_code != NORMAL_EXIT) {
      LOG.error(String.format("Command '%s' failed with exitcode %s", mergedCommand, exit_code));
    }
    return output_of_process;
  }

  public static Object executeCommandLocalHost(
      String command, boolean printToConsole, ProcessData.Types_Of_Data type) {
    return executeCommandLocalHost(
        new String[] {"bash", "-c", command}, printToConsole, type, DEFAULT_BEHAVIOUR_ON_ERRORS);
  }
}
