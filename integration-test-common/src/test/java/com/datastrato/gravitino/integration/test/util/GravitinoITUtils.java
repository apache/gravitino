/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoITUtils {
  public static final Logger LOG = LoggerFactory.getLogger(GravitinoITUtils.class);

  private GravitinoITUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void startGravitinoServer() {
    String gravitinoStartShell = System.getenv("GRAVITINO_HOME") + "/bin/gravitino.sh";
    String modifiedGravitinoStartShell =
        System.getenv("GRAVITINO_HOME") + String.format("/bin/gravitino_%s.sh", UUID.randomUUID());
    String krb5Path = System.getProperty("java.security.krb5.conf");
    if (krb5Path != null) {
      LOG.info("java.security.krb5.conf: {}", krb5Path);

      // Replace '/etc/krb5.conf' with the one in the test
      try {
        String content =
            FileUtils.readFileToString(new File(gravitinoStartShell), StandardCharsets.UTF_8);
        LOG.info("Before content: \n{}", content);
        content =
            content.replace(
                "#JAVA_OPTS+=\" -Djava.securit.krb5.conf=/etc/krb5.conf\"",
                String.format("JAVA_OPTS+=\" -Djava.security.krb5.conf=%s\"", krb5Path));
        FileUtils.write(new File(modifiedGravitinoStartShell), content, StandardCharsets.UTF_8);
        LOG.info("modifiedGravitinoStartShell content: \n{}", content);
      } catch (Exception e) {
        LOG.error("Can replace /etc/krb5.conf with real kerberos configuration", e);
      }
    }

    CommandExecutor.executeCommandLocalHost(
        modifiedGravitinoStartShell + " start", false, ProcessData.TypesOfData.OUTPUT);
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
