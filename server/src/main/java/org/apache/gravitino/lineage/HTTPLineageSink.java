/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.TransportFactory;
import io.openlineage.server.OpenLineage.RunEvent;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPLineageSink implements LineageSink {

  private static final Logger LOG = LoggerFactory.getLogger(LineageLogSinker.class);

  private OpenLineageClient client;

  @Override
  public void initialize(Map<String, String> properties) {
    HttpConfig httpConfig = new HttpConfig();
    HTTPLineageSinkConfig sinkConfig = new HTTPLineageSinkConfig(properties);
    try {
      String url = sinkConfig.get(HTTPLineageSinkConfig.URL);
      httpConfig.setUrl(new URI(url));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    int timeout = sinkConfig.get(HTTPLineageSinkConfig.TIMEOUT);
    httpConfig.setTimeoutInMillis(timeout);
    this.client =
        OpenLineageClient.builder().transport(new TransportFactory(httpConfig).build()).build();
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sink(RunEvent event) {
    try {
      OpenLineage.RunEvent clientEvent = RunEventUtils.getClientRunEvent(event);
      client.emit(clientEvent);
    } catch (JsonProcessingException e) {
      LOG.warn("");
    }
  }
}
