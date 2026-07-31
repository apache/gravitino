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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.SecretProviderInfo;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves {@code GET /configs/secrets/providers} with safe secrets-provider discovery metadata.
 *
 * <p>Uses the same auth model as {@link ConfigServlet} (no additional privilege check). Returns
 * only {@code name}, {@code type}, and optional non-secret {@code uri}.
 */
public class SecretProvidersConfigServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(SecretProvidersConfigServlet.class);

  private final List<SecretProviderInfo> providers;

  /**
   * Creates a servlet that lists providers configured in {@code config}.
   *
   * <p>Providers are loaded only to resolve safe metadata ({@code type()} / optional {@code uri}),
   * then closed. The discovery response is a static snapshot of configuration.
   *
   * @param config Gravitino server configuration
   */
  public SecretProvidersConfigServlet(Config config) {
    try (SecretProviderRegistry registry = new SecretProviderRegistry(config)) {
      this.providers = ImmutableList.copyOf(registry.listProviders());
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    Map<String, Object> body = ImmutableMap.of("providers", providers);
    try (PrintWriter writer = res.getWriter()) {
      res.setContentType("application/json;charset=utf-8");
      writer.write(ObjectMapperProvider.objectMapper().writeValueAsString(body));
    } catch (IllegalStateException exception) {
      LOG.error("Illegal state occurred when calling getWriter()", exception);
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      sendErrorResponse(res, "Failed to get response writer");
    } catch (IOException exception) {
      LOG.error("Failed to perform IO", exception);
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      sendErrorResponse(res, "IO error occurred");
    } catch (Exception e) {
      LOG.error("Unexpected error: {}", e.getMessage(), e);
      res.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      sendErrorResponse(res, "Internal server error");
    }
  }

  private void sendErrorResponse(HttpServletResponse res, String message) {
    try (PrintWriter writer = res.getWriter()) {
      res.setContentType("application/json;charset=utf-8");
      Map<String, String> error = Map.of("error", message);
      writer.write(ObjectMapperProvider.objectMapper().writeValueAsString(error));
    } catch (IOException e) {
      LOG.error("Failed to send error response", e);
    } catch (IllegalStateException e) {
      LOG.error("Failed to send error response: illegal state", e);
    }
  }
}
