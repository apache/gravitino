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
package org.apache.gravitino.paimon.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogRequest;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager;
import org.apache.gravitino.paimon.service.PaimonRESTUtils;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.ConfigResponse;

/**
 * The {@code GET /v1/config} endpoint of the Paimon REST protocol. It is the first request every
 * Paimon {@code RESTCatalog} client sends; the returned {@code prefix} default drives all
 * subsequent request paths.
 *
 * <p>Catalog options that clients need for data access (for example object storage endpoints) can
 * be forwarded with a {@code client.} prefix: a service configuration entry {@code
 * client.fs.s3a.endpoint} is returned to clients as {@code fs.s3a.endpoint}.
 */
@Path("/v1/config")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PaimonConfigOperations {

  private static final String PREFIX_KEY = "prefix";
  private static final String WAREHOUSE_KEY = "warehouse";
  private static final String CLIENT_CONFIG_PREFIX = "client.";

  private final PaimonCatalogRequest catalogs;

  @Inject
  public PaimonConfigOperations(PaimonCatalogRequest catalogs) {
    this.catalogs = catalogs;
  }

  /**
   * Returns the catalog configuration for a client.
   *
   * @param warehouse the warehouse the client requests, used to select a configured catalog
   * @return the config response
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "config." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "config", absolute = true)
  public Response getConfig(@QueryParam("warehouse") String warehouse) {
    String catalogName =
        warehouse == null ? PaimonCatalogWrapperManager.DEFAULT_CATALOG_NAME : warehouse;
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName);

    Map<String, String> defaults = new HashMap<>();
    Options options = wrapper.catalogOptions();
    String warehouseValue = options.get(WAREHOUSE_KEY);
    if (warehouseValue != null) {
      defaults.put(WAREHOUSE_KEY, warehouseValue);
    }
    if (options.toMap().keySet().stream()
        .anyMatch(
            key ->
                key.startsWith(CLIENT_CONFIG_PREFIX)
                    && SecretPropertyUtils.isSensitivePropertyKey(
                        key.substring(CLIENT_CONFIG_PREFIX.length())))) {
      catalogs.requireCredentials(catalogName);
    }
    options.toMap().entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(CLIENT_CONFIG_PREFIX))
        .forEach(
            entry ->
                defaults.put(
                    entry.getKey().substring(CLIENT_CONFIG_PREFIX.length()), entry.getValue()));

    // The advertised prefix routes subsequent requests to the selected catalog.
    defaults.put(PREFIX_KEY, catalogName);

    // Tell clients to fetch storage credentials per table when the catalog opts in. The
    // credentials themselves are never part of the config response; clients request them
    // from the table token endpoint.
    if (Boolean.parseBoolean(options.toMap().getOrDefault("data-token.enabled", "false"))) {
      defaults.put("data-token.enabled", "true");
    }

    return PaimonRESTUtils.ok(new ConfigResponse(ImmutableMap.copyOf(defaults), ImmutableMap.of()));
  }
}
