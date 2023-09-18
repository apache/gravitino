/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonITUtils {
  public static final Logger LOG = LoggerFactory.getLogger(GravitonITUtils.class);

  public static String HIVE_METASTORE_URIS = "thrift://localhost:9083";

  private GravitonITUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void startGravitonServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITON_HOME") + "/bin/graviton.sh start",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to start.
    sleep(3000, false);
  }

  public static void stopGravitonServer() {
    CommandExecutor.executeCommandLocalHost(
        System.getenv("GRAVITON_HOME") + "/bin/graviton.sh stop",
        false,
        ProcessData.TypesOfData.OUTPUT);
    // wait for server to stop.
    sleep(1000, false);
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

  public static HiveConf hiveConfig() {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);

    return hiveConf;
  }

  public static Map<String, String> hiveConfigProperties() {
    Map<String, String> catalogProps = Maps.newHashMap();
    catalogProps.put("provider", "hive");
    catalogProps.put(HiveConf.ConfVars.METASTOREURIS.varname, HIVE_METASTORE_URIS);
    catalogProps.put(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, "30");
    catalogProps.put(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES.varname, "30");
    catalogProps.put(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname, "5");

    return catalogProps;
  }
}
