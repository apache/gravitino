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

package org.apache.gravitino.server.web;

import java.nio.file.Path;
import java.util.Objects;
import javax.servlet.http.HttpServlet;
import org.apache.gravitino.Config;
import org.apache.gravitino.rest.RESTUtils;

public final class TestTlsServerUtils {

  public static final String TEST_STORE_PASSWORD = "changeit";

  private TestTlsServerUtils() {}

  public static int startHttpsServer(
      JettyServer server, boolean clientAuth, HttpServlet servlet, String servletPath)
      throws Exception {

    int port = RESTUtils.findAvailablePort(6000, 7000);

    Config config = new Config(false) {};
    config.set(JettyServerConfig.ENABLE_HTTPS, true);
    config.set(JettyServerConfig.WEBSERVER_HTTPS_PORT, port);

    config.set(
        JettyServerConfig.SSL_KEYSTORE_PATH, testResource("test-server-keystore.p12").toString());
    config.set(JettyServerConfig.SSL_KEYSTORE_PASSWORD, TEST_STORE_PASSWORD);
    config.set(JettyServerConfig.SSL_MANAGER_PASSWORD, TEST_STORE_PASSWORD);

    config.set(JettyServerConfig.ENABLE_CLIENT_AUTH, clientAuth);

    if (clientAuth) {
      config.set(
          JettyServerConfig.SSL_TRUST_STORE_PATH,
          testResource("test-server-truststore.p12").toString());
      config.set(JettyServerConfig.SSL_TRUST_STORE_PASSWORD, TEST_STORE_PASSWORD);
    }

    server.initialize(JettyServerConfig.fromConfig(config), "test", false);
    server.start();
    server.addServlet(servlet, servletPath);

    return port;
  }

  public static Path testResource(String filename) throws Exception {
    return Path.of(
        Objects.requireNonNull(
                TestTlsServerUtils.class.getResource("/tls/" + filename),
                "Missing TLS test resource: " + filename)
            .toURI());
  }
}
