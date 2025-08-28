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
package org.apache.gravitino.lineage.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.server.OpenLineage;
import java.util.Map;
import org.apache.gravitino.lineage.Utils;
import org.apache.gravitino.lineage.auth.AuthenticationFactory;
import org.apache.gravitino.lineage.auth.LineageServerAuthenticationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageHttpSink implements LineageSink {

  private static final Logger LOG = LoggerFactory.getLogger(LineageHttpSink.class);
  private OpenLineageClient client;

  @Override
  public void initialize(Map<String, String> configs) {
    String httpSinkUrl = configs.getOrDefault("url", "http://localhost:5000/");
    String authType = configs.get("authType");
    LOG.info("Http sink URL: {}, authentication type: {}", httpSinkUrl, authType);
    LineageServerAuthenticationStrategy authStrategy =
        AuthenticationFactory.createStrategy(authType);

    HttpConfig httpConfig = authStrategy.configureHttpConfig(httpSinkUrl, configs);

    HttpTransport transport = new HttpTransport(httpConfig);

    client = OpenLineageClient.builder().transport(transport).build();
  }

  @Override
  public void sink(OpenLineage.RunEvent runEvent) {
    try {
      client.emit(Utils.getClientRunEvent(runEvent));
      LOG.info("Sent lineage event to http sink: {}", runEvent);
    } catch (JsonProcessingException e) {
      LOG.warn("Could not parse lineage run event", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
