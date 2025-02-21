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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Configs;
import org.apache.gravitino.server.ServerConfig;
import org.junit.jupiter.api.Test;

public class TestConfigServlet {

  @Test
  public void testConfigServlet() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    ConfigServlet configServlet = new ConfigServlet(serverConfig);
    configServlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    configServlet.doGet(null, res);
    verify(writer)
        .write(
            "{\"gravitino.authorization.enable\":false,\"gravitino.authenticators\":[\"simple\"]}");
    configServlet.destroy();
  }

  @Test
  public void testConfigServletWithVisibleConfigs() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.set(
        Configs.VISIBLE_CONFIGS,
        Lists.newArrayList(Configs.AUDIT_LOG_FORMATTER_CLASS_NAME.getKey()));
    serverConfig.set(Configs.AUDIT_LOG_FORMATTER_CLASS_NAME, "test");
    ConfigServlet configServlet = new ConfigServlet(serverConfig);
    configServlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    configServlet.doGet(null, res);
    verify(writer)
        .write(
            "{\"gravitino.audit.formatter.className\":\"test\",\"gravitino.authorization.enable\":false,\"gravitino.authenticators\":[\"simple\"]}");
    configServlet.destroy();
  }
}
