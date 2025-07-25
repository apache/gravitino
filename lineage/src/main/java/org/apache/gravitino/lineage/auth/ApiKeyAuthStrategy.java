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
package org.apache.gravitino.lineage.auth;

import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.TokenProvider;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApiKeyAuthStrategy implements LineageServerAuthenticationStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(ApiKeyAuthStrategy.class);

  @Override
  public HttpConfig configureHttpConfig(String url, Map<String, String> configs) {
    String apiKey = configs.get("apiKey");
    if (apiKey == null || apiKey.trim().isEmpty()) {
      throw new BadRequestException("API key is required when auth type is apiKey");
    }

    HttpConfig config = new HttpConfig();
    config.setUrl(URI.create(url));

    // Create TokenProvider for API key
    TokenProvider tokenProvider =
        new TokenProvider() {
          @Override
          public String getToken() {
            return apiKey;
          }
        };
    config.setAuth(tokenProvider);

    LOG.info("Configured API key authentication for OpenLineage client");
    return config;
  }
}
