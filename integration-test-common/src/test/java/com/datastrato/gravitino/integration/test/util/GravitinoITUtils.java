/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoITUtils {
  public static final Logger LOG = LoggerFactory.getLogger(GravitinoITUtils.class);

  private GravitinoITUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void startGravitinoServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITINO_HOME") + "/bin/gravitino.sh start",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to start.
    sleep(3000, false);
  }

  public static void stopGravitinoServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITINO_HOME") + "/bin/gravitino.sh stop",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to stop.
    sleep(1000, false);
  }

  public static void sleep(long millis, boolean logOutput) {
    if (logOutput && LOG.isInfoEnabled()) {
      LOG.info("Starting sleeping for {} milliseconds ...", millis);
      LOG.info("Caller: {}", Thread.currentThread().getStackTrace()[2]);
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
    return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
  }
}
