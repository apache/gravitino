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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import io.airlift.log.Logger;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.GravitinoConfig;

/**
 * Periodically asks the Gravitino server whether it has an Iceberg REST server running for a
 * metalake, and caches the answer on the shared {@link GravitinoConfig} for {@link
 * IcebergConnectorAdapter} to read on the next catalog load. Owns the Iceberg-specific decision of
 * whether discovery is needed at all (routing enabled, no manual override configured) so that the
 * generic catalog connector manager only needs to call {@link #refresh} once per poll.
 */
public class IcebergRestUriDiscovery {

  private static final Logger LOG = Logger.get(IcebergRestUriDiscovery.class);

  // Tracks which metalakes' discovery is currently failing, so a failure is logged at INFO only
  // on the transition out of that state rather than on every poll.
  private final Set<String> failing = ConcurrentHashMap.newKeySet();

  /**
   * Refreshes the discovered Iceberg REST server endpoint for {@code metalakeName}, if Iceberg REST
   * routing is enabled and no manual endpoint is configured for it. Failures — including talking to
   * a Gravitino server older than this endpoint — must not interrupt catalog loading, so they are
   * swallowed here; Iceberg catalogs simply keep their last known routing decision until the next
   * successful poll. A failure is logged at ERROR on every poll because routing through Iceberg
   * REST is required when enabled.
   *
   * @param metalakeName the metalake to refresh the discovered endpoint for
   * @param config the connector configuration to read routing settings from and cache the result on
   * @param client the Gravitino client to query
   */
  public void refresh(String metalakeName, GravitinoConfig config, GravitinoAdminClient client) {
    if (!config.isIcebergRestRoutingEnabled()
        || StringUtils.isNotBlank(config.getManualIcebergRestUri(metalakeName))) {
      return;
    }
    try {
      config.setDiscoveredIcebergRestUri(
          metalakeName, client.icebergRestServiceUri(metalakeName).orElse(null));
      if (failing.remove(metalakeName)) {
        LOG.info("Iceberg REST service discovery for metalake %s recovered.", metalakeName);
      }
    } catch (Exception e) {
      failing.add(metalakeName);
      LOG.error(
          e,
          "Failed to query the Iceberg REST service endpoint for metalake %s; Iceberg catalogs "
              + "without a configured REST endpoint cannot be registered until discovery "
              + "recovers. Set gravitino.iceberg.rest-uri explicitly, upgrade the Gravitino "
              + "server to one that supports discovery, or disable Iceberg REST routing with "
              + "gravitino.iceberg.rest-routing-enabled=false to use legacy backend translation.",
          metalakeName);
    }
  }
}
