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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergCatalogOperations extends IcebergTestBase {

  private static final String CATALOGS_PATH = "gravitino/v1/management/catalogs";
  private static final IcebergConfigProvider CONFIG_PROVIDER = mock(IcebergConfigProvider.class);

  @Override
  protected Application configure() {
    return new ResourceConfig()
        .register(new IcebergCatalogOperations(CONFIG_PROVIDER))
        .register(IcebergObjectMapperProvider.class)
        .register(JacksonFeature.class)
        .register(IcebergExceptionMapper.class);
  }

  @AfterEach
  public void resetProvider() {
    Mockito.reset(CONFIG_PROVIDER);
  }

  @Test
  public void testListCatalogs() {
    when(CONFIG_PROVIDER.listCatalogs()).thenReturn(new String[] {"zeta", "alpha", "zeta"});

    Response response = getIcebergClientBuilder(CATALOGS_PATH, Optional.empty()).get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
    Assertions.assertEquals(
        "{\"catalogs\":[{\"name\":\"alpha\"},{\"name\":\"zeta\"}]}",
        response.readEntity(String.class));
  }

  @Test
  public void testListCatalogsWhenEmpty() {
    when(CONFIG_PROVIDER.listCatalogs()).thenReturn(new String[0]);

    Response response = getIcebergClientBuilder(CATALOGS_PATH, Optional.empty()).get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(
        0, response.readEntity(IcebergCatalogListResponse.class).catalogs().length);
  }

  @Test
  public void testListCatalogsFailure() {
    when(CONFIG_PROVIDER.listCatalogs()).thenThrow(new IllegalStateException("list failed"));

    Response response = getIcebergClientBuilder(CATALOGS_PATH, Optional.empty()).get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    ErrorResponse error = response.readEntity(ErrorResponse.class);
    Assertions.assertEquals("IllegalStateException", error.type());
    Assertions.assertEquals("list failed", error.message());
  }
}
