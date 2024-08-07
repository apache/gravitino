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

import java.util.Map;
import org.apache.gravitino.Config;
import org.eclipse.jetty.servlet.FilterHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCorsFilterHolder {

  @Test
  public void testCreateCorsFilterHolder() {
    Config config = new Config() {};
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    FilterHolder filterHolder = CorsFilterHolder.create(jettyServerConfig);
    Map<String, String> parameters = filterHolder.getInitParameters();
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_ORIGINS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_ORIGINS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_TIMING_ORIGINS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_TIMING_ORIGINS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.ALLOW_CREDENTIALS.getDefaultValue()),
        parameters.get(JettyServerConfig.ALLOW_CREDENTIALS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_HEADERS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_HEADERS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.CHAIN_PREFLIGHT.getDefaultValue()),
        parameters.get(JettyServerConfig.CHAIN_PREFLIGHT.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.EXPOSED_HEADERS.getDefaultValue(),
        parameters.get(JettyServerConfig.EXPOSED_HEADERS.getKey()));
    Assertions.assertEquals(
        JettyServerConfig.ALLOWED_METHODS.getDefaultValue(),
        parameters.get(JettyServerConfig.ALLOWED_METHODS.getKey()));
    Assertions.assertEquals(
        String.valueOf(JettyServerConfig.PREFLIGHT_MAX_AGE_IN_SECS.getDefaultValue()),
        parameters.get(CorsFilterHolder.PREFLIGHT_MAX_AGE));
    Assertions.assertEquals(
        "org.eclipse.jetty.servlets.CrossOriginFilter", filterHolder.getClassName());
    config.set(JettyServerConfig.ALLOWED_ORIGINS, "a");
    config.set(JettyServerConfig.ALLOWED_TIMING_ORIGINS, "b");
    config.set(JettyServerConfig.ALLOWED_HEADERS, "c");
    config.set(JettyServerConfig.ALLOWED_METHODS, "d");
    config.set(JettyServerConfig.EXPOSED_HEADERS, "e");
    config.set(JettyServerConfig.ALLOW_CREDENTIALS, false);
    config.set(JettyServerConfig.CHAIN_PREFLIGHT, false);
    config.set(JettyServerConfig.PREFLIGHT_MAX_AGE_IN_SECS, 10);
    jettyServerConfig = JettyServerConfig.fromConfig(config, "");
    filterHolder = CorsFilterHolder.create(jettyServerConfig);
    parameters = filterHolder.getInitParameters();
    Assertions.assertEquals("a", parameters.get(JettyServerConfig.ALLOWED_ORIGINS.getKey()));
    Assertions.assertEquals("b", parameters.get(JettyServerConfig.ALLOWED_TIMING_ORIGINS.getKey()));
    Assertions.assertEquals("false", parameters.get(JettyServerConfig.ALLOW_CREDENTIALS.getKey()));
    Assertions.assertEquals("c", parameters.get(JettyServerConfig.ALLOWED_HEADERS.getKey()));
    Assertions.assertEquals("false", parameters.get(JettyServerConfig.CHAIN_PREFLIGHT.getKey()));
    Assertions.assertEquals("e", parameters.get(JettyServerConfig.EXPOSED_HEADERS.getKey()));
    Assertions.assertEquals("d", parameters.get(JettyServerConfig.ALLOWED_METHODS.getKey()));
    Assertions.assertNull(parameters.get(JettyServerConfig.PREFLIGHT_MAX_AGE_IN_SECS.getKey()));

    Assertions.assertEquals("10", parameters.get(CorsFilterHolder.PREFLIGHT_MAX_AGE));
  }
}
