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
package org.apache.gravitino.lance;

import java.util.Map;
import javax.servlet.Servlet;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceCatalogService;
import org.apache.gravitino.lance.service.rest.LanceNamespaceOperations;
import org.apache.gravitino.server.web.JettyServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thin REST service shell for Lance metadata. */
public class LanceRESTService implements GravitinoAuxiliaryService {

  private static final Logger LOG = LoggerFactory.getLogger(LanceRESTService.class);

  public static final String SERVICE_NAME = "lance-rest";
  public static final String LANCE_SPEC = "/lance/*";

  private JettyServer server;
  private LanceCatalogService catalogService;

  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  @Override
  public void serviceInit(Map<String, String> properties) {
    LanceConfig lanceConfig = new LanceConfig(properties);
    JettyServerConfig serverConfig = JettyServerConfig.fromConfig(lanceConfig);

    server = new JettyServer();
    server.initialize(serverConfig, SERVICE_NAME, false);

    catalogService = new LanceCatalogService(lanceConfig);

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(JacksonFeature.class);
    resourceConfig.register(new LanceNamespaceOperations(catalogService));

    Servlet container = new ServletContainer(resourceConfig);
    server.addServlet(container, LANCE_SPEC);
    server.addCustomFilters(LANCE_SPEC);
    server.addSystemFilters(LANCE_SPEC);

    LOG.info("Initialized Lance REST service for catalog {}", lanceConfig.getCatalogName());
  }

  @Override
  public void serviceStart() {
    if (server != null) {
      server.start();
      LOG.info("Lance REST service started");
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (server != null) {
      server.stop();
      LOG.info("Lance REST service stopped");
    }
    if (catalogService != null) {
      catalogService.close();
    }
  }

  public void join() {
    if (server != null) {
      server.join();
    }
  }
}
