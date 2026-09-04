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
package org.apache.gravitino.iceberg;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Set;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.server.web.HttpAuditFilter;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.JettyServerTestUtils;
import org.apache.gravitino.server.web.RequestContextFilter;
import org.eclipse.jetty.servlet.ServletHandler;
import org.junit.jupiter.api.Test;

public class TestRESTService {

  /**
   * RESTService.initServer() previously registered /metrics and /prometheus/metrics (added by
   * JettyServer#initialize() itself, outside ICEBERG_SPEC) with no audit coverage at all, and
   * nothing in the build caught it. Rather than parse source text, this exercises the extracted
   * RESTService#registerMetricsPathFilters against a plain JettyServer and inspects the real
   * ServletHandler filter mappings it produces, so a broken JettyServer.METRICS_PATH_SPECS list or
   * a filter that merely appears in a comment cannot pass. See GH-12760.
   */
  @Test
  public void testMetricsPathsHaveAuditFilterCoverage() throws Exception {
    JettyServer server = new JettyServer();
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(new IcebergConfig());
    server.initialize(jettyServerConfig, "test-iceberg-rest", false);
    EventBus eventBus = new EventBus(Collections.emptyList());

    RESTService.registerMetricsPathFilters(server, eventBus);

    ServletHandler servletHandler =
        JettyServerTestUtils.getServletContextHandler(server).getServletHandler();
    Set<String> auditedPathSpecs =
        JettyServerTestUtils.filterPathSpecsFor(servletHandler, HttpAuditFilter.class);
    Set<String> requestContextPathSpecs =
        JettyServerTestUtils.filterPathSpecsFor(servletHandler, RequestContextFilter.class);

    for (String pathSpec : JettyServer.METRICS_PATH_SPECS) {
      assertTrue(
          auditedPathSpecs.contains(pathSpec),
          "'" + pathSpec + "' must be covered by HttpAuditFilter, see GH-12760");
      assertTrue(
          requestContextPathSpecs.contains(pathSpec),
          "'"
              + pathSpec
              + "' must be covered by RequestContextFilter for query-parameter "
              + "capture, see GH-12760");
    }

    server.stop();
  }
}
