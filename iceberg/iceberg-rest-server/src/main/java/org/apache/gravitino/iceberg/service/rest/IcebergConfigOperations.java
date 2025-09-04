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
package org.apache.gravitino.iceberg.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;

@Path("/v1/{prefix:([^/]*/)?}config")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergConfigOperations {

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  private final IcebergCatalogWrapperManager catalogWrapperManager;

  private static final List<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableList.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_NAMESPACE_EXISTS)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_TABLE_EXISTS)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  private static final List<Endpoint> DEFAULT_VIEW_ENDPOINTS =
      ImmutableList.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .add(Endpoint.V1_VIEW_EXISTS)
          .build();

  @Inject
  public IcebergConfigOperations(IcebergCatalogWrapperManager catalogWrapperManager) {
    this.catalogWrapperManager = catalogWrapperManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "config." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "config", absolute = true)
  public Response getConfig(@DefaultValue("") @QueryParam("warehouse") String warehouse) {
    String catalogName = getCatalogName(warehouse);
    boolean supportsView = supportsViewOperations(catalogName);
    ConfigResponse.Builder builder = ConfigResponse.builder();
    builder.withDefaults(getDefaultConfig(catalogName)).withEndpoints(getEndpoints(supportsView));
    if (StringUtils.isNotBlank(warehouse)) {
      builder.withDefault("prefix", warehouse);
    }
    return IcebergRestUtils.ok(builder.build());
  }

  private List<Endpoint> getEndpoints(boolean supportsViewOperations) {
    if (!supportsViewOperations) {
      return DEFAULT_ENDPOINTS;
    }
    return Stream.concat(DEFAULT_ENDPOINTS.stream(), DEFAULT_VIEW_ENDPOINTS.stream())
        .collect(Collectors.toList());
  }

  private Map<String, String> getCatalogConfig(String catalogName) {
    Map<String, String> configs = new HashMap<>();
    CatalogWrapperForREST catalogWrapper = getCatalogWrapper(catalogName);
    configs.putAll(catalogWrapper.getCatalogConfigToClient());
    return configs;
  }

  private String getCatalogName(String warehouse) {
    if (StringUtils.isBlank(warehouse)) {
      return IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG;
    } else {
      return warehouse;
    }
  }

  private boolean supportsViewOperations(String catalogName) {
    CatalogWrapperForREST catalogWrapperForREST = getCatalogWrapper(catalogName);
    return catalogWrapperForREST.supportsViewOperations();
  }

  private CatalogWrapperForREST getCatalogWrapper(String catalogName) {
    return catalogWrapperManager.getCatalogWrapper(catalogName);
  }

  protected Map<String, String> getDefaultConfig(String catalogName) {
    return getCatalogConfig(catalogName);
  }

  protected ConfigResponse.Builder getConfigResponseBuilder(boolean supportsViewOperations) {
    return ConfigResponse.builder();
  }
}
