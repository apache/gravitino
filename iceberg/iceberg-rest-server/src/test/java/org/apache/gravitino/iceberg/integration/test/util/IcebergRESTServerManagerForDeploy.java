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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.integration.test.util.CommandExecutor;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ProcessData;
import org.apache.gravitino.integration.test.util.ProcessData.TypesOfData;

public class IcebergRESTServerManagerForDeploy extends IcebergRESTServerManager {

  private static final String SCRIPT_NAME = "gravitino-iceberg-rest-server.sh";
  private static final String KRB5_CONF_PLACEHOLDER =
      "#JAVA_OPTS+=\" -Djava.security.krb5.conf=/etc/krb5.conf\"";
  private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
  private static final String KRB5_REALM_PROPERTY = "java.security.krb5.realm";
  private static final String KRB5_KDC_PROPERTY = "java.security.krb5.kdc";
  private static final String SQLITE_DRIVER_DOWNLOAD_URL =
      "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.42.0.0/sqlite-jdbc-3.42.0.0.jar";

  private final Path icebergRESTServerHome;

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
    String startShell = gravitinoRestStartShell;
    String krb5Path = System.getProperty(KRB5_CONF_PROPERTY);
    if (krb5Path != null) {
      LOG.info("java.security.krb5.conf: {}", krb5Path);
      startShell = prepareKerberosStartShell(gravitinoRestStartShell, krb5Path);
    }

    CommandExecutor.executeCommandLocalHost(
        startShell + " start", false, ProcessData.TypesOfData.OUTPUT, deployEnvironment());
    return Optional.empty();
  }

  @Override
  public void doStopIcebergRESTServer() {
    String cmd = String.format("%s/bin/%s stop", icebergRESTServerHome.toString(), SCRIPT_NAME);
    CommandExecutor.executeCommandLocalHost(cmd, false, TypesOfData.ERROR, deployEnvironment());
  }

  private Map<String, String> deployEnvironment() {
    return ImmutableMap.of("GRAVITINO_HOME", icebergRESTServerHome.toString());
  }

  private String prepareKerberosStartShell(String gravitinoRestStartShell, String krb5Path)
      throws Exception {
    String modifiedGravitinoStartShell =
        String.format(
            "%s/bin/gravitino-iceberg-rest-server_%s.sh",
            icebergRESTServerHome.toString(), UUID.randomUUID());
    String content =
        FileUtils.readFileToString(new File(gravitinoRestStartShell), StandardCharsets.UTF_8);
    if (!content.contains(KRB5_CONF_PLACEHOLDER)) {
      throw new IllegalStateException(
          String.format(
              "Failed to patch Kerberos options in %s: missing placeholder %s",
              gravitinoRestStartShell, KRB5_CONF_PLACEHOLDER));
    }
    content =
        content.replace(
            KRB5_CONF_PLACEHOLDER,
            String.format("JAVA_OPTS+=\"%s\"", buildKerberosJavaOpts(krb5Path)));
    File tmp = new File(modifiedGravitinoStartShell);
    FileUtils.write(tmp, content, StandardCharsets.UTF_8);
    tmp.setExecutable(true);
    LOG.info("modifiedGravitinoStartShell content: \n{}", content);
    return modifiedGravitinoStartShell;
  }

  private String buildKerberosJavaOpts(String krb5Path) {
    StringBuilder opts = new StringBuilder();
    opts.append(String.format(" -Djava.security.krb5.conf=%s", krb5Path));
    String realm = System.getProperty(KRB5_REALM_PROPERTY);
    if (realm != null) {
      opts.append(String.format(" -Djava.security.krb5.realm=%s", realm));
    }
    String kdc = System.getProperty(KRB5_KDC_PROPERTY);
    if (kdc != null) {
      opts.append(String.format(" -Djava.security.krb5.kdc=%s", kdc));
    }
    return opts.toString();
  }
}
