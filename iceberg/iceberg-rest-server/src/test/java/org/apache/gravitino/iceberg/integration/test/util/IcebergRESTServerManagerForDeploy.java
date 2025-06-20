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
package org.apache.gravitino.iceberg.integration.test.util;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.gravitino.integration.test.util.ProcessData.TypesOfData;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class IcebergRESTServerManagerForDeploy extends IcebergRESTServerManager {

  private static final String SCRIPT_NAME = "gravitino-iceberg-rest-server.sh";
  private Path icebergRESTServerHome;
  private static final String SQLITE_DRIVER_DOWNLOAD_URL =
      "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.42.0.0/sqlite-jdbc-3.42.0.0.jar";

  public IcebergRESTServerManagerForDeploy() {
    String gravitinoRootDir = System.getenv("GRAVITINO_ROOT_DIR");
    this.icebergRESTServerHome = Paths.get(gravitinoRootDir, "distribution", "package");
  }

  @Override
  public Path getConfigDir() {
    return Paths.get(icebergRESTServerHome.toString(), "conf");
  }

  @Override
  public Optional<Future<?>> doStartIcebergRESTServer() throws Exception {
    DownloaderUtils.downloadFile(
        SQLITE_DRIVER_DOWNLOAD_URL,
        Paths.get(icebergRESTServerHome.toString(), "iceberg-rest-server", "libs").toString());

    String gravitinoRestStartShell = icebergRESTServerHome.toString() + "/bin/" + SCRIPT_NAME;
    String krb5Path = System.getProperty("java.security.krb5.conf");
    if (krb5Path != null) {
      LOG.info("java.security.krb5.conf: {}", krb5Path);
      String modifiedGravitinoStartShell =
          String.format(
              "%s/bin/gravitino-iceberg-rest-server_%s.sh",
              icebergRESTServerHome.toString(), UUID.randomUUID());
      // Replace '/etc/krb5.conf' with the one in the test
      try {
        String content =
            FileUtils.readFileToString(new File(gravitinoRestStartShell), StandardCharsets.UTF_8);
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
      String cmd = String.format("%s/bin/%s start", icebergRESTServerHome.toString(), SCRIPT_NAME);
      CommandExecutor.executeCommandLocalHost(
          cmd,
          false,
          ProcessData.TypesOfData.OUTPUT,
          ImmutableMap.of("GRAVITINO_HOME", icebergRESTServerHome.toString()));
    }
    return Optional.empty();
  }

  @Override
  public void doStopIcebergRESTServer() {
    String cmd = String.format("%s/bin/%s stop", icebergRESTServerHome.toString(), SCRIPT_NAME);
    CommandExecutor.executeCommandLocalHost(cmd, false, TypesOfData.ERROR);
  }
}
