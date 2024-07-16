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
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsCatalogs;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/**
 * Apache Gravitino Metalake is the top-level metadata repository for users. It contains a list of
 * catalogs as sub-level metadata collections. With {@link GravitinoMetalake}, users can list,
 * create, load, alter and drop a catalog with specified identifier.
 */
public class GravitinoMetalake extends MetalakeDTO implements SupportsCatalogs {
  private static final String API_METALAKES_CATALOGS_PATH = "api/metalakes/%s/catalogs/%s";

  private final RESTClient restClient;

  GravitinoMetalake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  /**
   * List all the catalogs under this metalake.
   *
   * @return A list of the catalog names under the current metalake.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  @Override
  public String[] listCatalogs() throws NoSuchMetalakeException {

    EntityListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", this.name()),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers()).map(NameIdentifier::name).toArray(String[]::new);
  }

  /**
   * List all the catalogs with their information under this metalake.
   *
   * @return A list of {@link Catalog} under the specified namespace.
   * @throws NoSuchMetalakeException if the metalake with specified namespace does not exist.
   */
  @Override
  public Catalog[] listCatalogsInfo() throws NoSuchMetalakeException {

    Map<String, String> params = new HashMap<>();
    params.put("details", "true");
    CatalogListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", this.name()),
            params,
            CatalogListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    return Arrays.stream(resp.getCatalogs())
        .map(c -> DTOConverters.toCatalog(this.name(), c, restClient))
        .toArray(Catalog[]::new);
  }

  /**
   * Load the catalog with specified identifier.
   *
   * @param catalogName The identifier of the catalog to load.
   * @return The {@link Catalog} with specified identifier.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   */
  @Override
  public Catalog loadCatalog(String catalogName) throws NoSuchCatalogException {

    CatalogResponse resp =
        restClient.get(
            String.format(API_METALAKES_CATALOGS_PATH, this.name(), catalogName),
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Create a new catalog with specified identifier, type, comment and properties.
   *
   * @param catalogName The identifier of the catalog.
   * @param type The type of the catalog.
   * @param provider The provider of the catalog.
   * @param comment The comment of the catalog.
   * @param properties The properties of the catalog.
   * @return The created {@link Catalog}.
   * @throws NoSuchMetalakeException if the metalake with specified namespace does not exist.
   * @throws CatalogAlreadyExistsException if the catalog with specified identifier already exists.
   */
  @Override
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {

    CatalogCreateRequest req =
        new CatalogCreateRequest(catalogName, type, provider, comment, properties);
    req.validate();

    CatalogResponse resp =
        restClient.post(
            String.format("api/metalakes/%s/catalogs", this.name()),
            req,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Alter the catalog with specified identifier by applying the changes.
   *
   * @param catalogName the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return the altered {@link Catalog}.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Catalog alterCatalog(String catalogName, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {

    List<CatalogUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toCatalogUpdateRequest)
            .collect(Collectors.toList());
    CatalogUpdatesRequest updatesRequest = new CatalogUpdatesRequest(reqs);
    updatesRequest.validate();

    CatalogResponse resp =
        restClient.put(
            String.format(API_METALAKES_CATALOGS_PATH, this.name(), catalogName),
            updatesRequest,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(this.name(), resp.getCatalog(), restClient);
  }

  /**
   * Drop the catalog with specified identifier.
   *
   * @param catalogName the name of the catalog.
   * @return true if the catalog is dropped successfully, false if the catalog does not exist.
   */
  @Override
  public boolean dropCatalog(String catalogName) {

    DropResponse resp =
        restClient.delete(
            String.format(API_METALAKES_CATALOGS_PATH, this.name(), catalogName),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * Test whether a catalog can be created successfully with the specified parameters, without
   * actually creating it.
   *
   * @param catalogName the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if the test failed.
   */
  @Override
  public void testConnection(
      String catalogName,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    CatalogCreateRequest req =
        new CatalogCreateRequest(catalogName, type, provider, comment, properties);
    req.validate();

    // The response maybe a `BaseResponse` (test successfully)  or an `ErrorResponse` (test failed),
    // we use the `ErrorResponse` here because it contains all fields of `BaseResponse` (code field
    // only)
    ErrorResponse resp =
        restClient.post(
            String.format("api/metalakes/%s/catalogs/testConnection", this.name()),
            req,
            ErrorResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());

    if (resp.getCode() == 0) {
      return;
    }

    // Throw the corresponding exception
    ErrorHandlers.catalogErrorHandler().accept(resp);
  }

  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    private Builder() {
      super();
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GravitinoMetalake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new GravitinoMetalake(name, comment, properties, audit, restClient);
    }
  }

  /** @return the builder for creating a new instance of GravitinoMetaLake. */
  public static Builder builder() {
    return new Builder();
  }
}
