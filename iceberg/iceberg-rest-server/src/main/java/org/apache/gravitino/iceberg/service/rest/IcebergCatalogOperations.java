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
import com.google.common.base.Preconditions;
import java.util.Arrays;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergCatalogListResponse.CatalogInfo;
import org.apache.gravitino.metrics.MetricNames;

/** Gravitino-private management operations for Iceberg REST catalogs. */
@Path("/gravitino/v1/management/catalogs")
@Produces(MediaType.APPLICATION_JSON)
public class IcebergCatalogOperations {

  private final IcebergConfigProvider configProvider;

  /**
   * Creates Iceberg catalog management operations.
   *
   * @param configProvider provider used to discover catalogs served by this REST server
   */
  @Inject
  public IcebergCatalogOperations(IcebergConfigProvider configProvider) {
    this.configProvider = configProvider;
  }

  /**
   * Lists catalogs served by this Iceberg REST server.
   *
   * @return catalog names accepted by the server's {@code warehouse} parameter
   */
  @GET
  @Timed(name = "list-rest-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-rest-catalog", absolute = true)
  public Response listCatalogs() {
    String[] catalogNames =
        Preconditions.checkNotNull(configProvider.listCatalogs(), "catalogs must not be null");
    CatalogInfo[] catalogs =
        Arrays.stream(catalogNames)
            .map(name -> Preconditions.checkNotNull(name, "catalog name must not be null"))
            .distinct()
            .sorted()
            .map(CatalogInfo::new)
            .toArray(CatalogInfo[]::new);
    return IcebergRESTUtils.ok(new IcebergCatalogListResponse(catalogs));
  }
}
