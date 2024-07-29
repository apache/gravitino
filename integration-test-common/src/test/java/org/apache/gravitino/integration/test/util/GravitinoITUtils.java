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
package org.apache.gravitino.integration.test.util;

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

    String krb5Path = System.getProperty("java.security.krb5.conf");
    if (krb5Path != null) {
      LOG.info("java.security.krb5.conf: {}", krb5Path);
      String modifiedGravitinoStartShell =
          System.getenv("GRAVITINO_HOME")
              + String.format("/bin/gravitino_%s.sh", UUID.randomUUID());
      // Replace '/etc/krb5.conf' with the one in the test
      try {
        String content =
            FileUtils.readFileToString(new File(gravitinoStartShell), StandardCharsets.UTF_8);
        content =
            content.replace(
                "#JAVA_OPTS+=\" -Djava.security.krb5.conf=/etc/krb5.conf\"",
                String.format("JAVA_OPTS+=\" -Djava.security.krb5.conf=%s\"", krb5Path));
        File tmp = new File(modifiedGravitinoStartShell);
        FileUtils.write(tmp, content, StandardCharsets.UTF_8);
        tmp.setExecutable(true);
        LOG.info("modifiedGravitinoStartShell content: \n{}", content);
        CommandExecutor.executeCommandLocalHost(
            modifiedGravitinoStartShell + " start", false, ProcessData.TypesOfData.OUTPUT);
      } catch (Exception e) {
        LOG.error("Can replace /etc/krb5.conf with real kerberos configuration", e);
      }
    } else {
      CommandExecutor.executeCommandLocalHost(
          gravitinoStartShell + " start", false, ProcessData.TypesOfData.OUTPUT);
    }
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
