/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonITUtils {
  public static final Logger LOG = LoggerFactory.getLogger(GravitonITUtils.class);

  public static void startGravitonServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITON_HOME") + "/bin/graviton.sh start",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to start.
    sleep(100, false);
  }

  public static void stopGravitonServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITON_HOME") + "/bin/graviton.sh stop",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to stop.
    sleep(100, false);
  }

  public static void sleep(long millis, boolean logOutput) {
    if (logOutput) {
      LOG.info("Starting sleeping for " + millis + " milliseconds ...");
      LOG.info("Caller: " + Thread.currentThread().getStackTrace()[2]);
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.error("Exception in sleep() ", e);
    }
    if (logOutput) {
      LOG.info("Finished.");
    }
  }

  public static String genRandomName(String prefix) {
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
