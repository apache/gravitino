/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.SupportsCatalogs;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.requests.CatalogCreateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdateRequest;
import com.datastrato.gravitino.dto.requests.CatalogUpdatesRequest;
import com.datastrato.gravitino.dto.responses.CatalogResponse;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Metalake is the top-level metadata repository for users. It contains a list of catalogs
 * as sub-level metadata collections. With {@link GravitinoMetaLake}, users can list, create, load,
 * alter and drop a catalog with specified identifier.
 */
public class GravitinoMetaLake extends MetalakeDTO implements SupportsCatalogs {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoMetaLake.class);

  private static final String API_METALAKES_CATALOGS_PATH = "api/metalakes/%s/catalogs/%s";

  private final RESTClient restClient;

  GravitinoMetaLake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  /**
   * List all the catalogs under this metalake with specified namespace.
   *
   * @param namespace The namespace to list the catalogs under it.
   * @return A list of {@link NameIdentifier} of the catalogs under the specified namespace.
   * @throws NoSuchMetalakeException if the metalake with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    Namespace.checkCatalog(namespace);

    EntityListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", namespace.level(0)),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Load the catalog with specified identifier.
   *
   * @param ident The identifier of the catalog to load.
   * @return The {@link Catalog} with specified identifier.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   */
  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    NameIdentifier.checkCatalog(ident);

    CatalogResponse resp =
        restClient.get(
            String.format(API_METALAKES_CATALOGS_PATH, ident.namespace().level(0), ident.name()),
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(resp.getCatalog(), restClient);
  }

  /**
   * Create a new catalog with specified identifier, type, comment and properties.
   *
   * @param ident The identifier of the catalog.
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
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    NameIdentifier.checkCatalog(ident);

    CatalogCreateRequest req =
        new CatalogCreateRequest(ident.name(), type, provider, comment, properties);
    req.validate();

    CatalogResponse resp =
        restClient.post(
            String.format("api/metalakes/%s/catalogs", ident.namespace().level(0)),
            req,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(resp.getCatalog(), restClient);
  }

  /**
   * Alter the catalog with specified identifier by applying the changes.
   *
   * @param ident the identifier of the catalog.
   * @param changes the changes to apply to the catalog.
   * @return the altered {@link Catalog}.
   * @throws NoSuchCatalogException if the catalog with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    NameIdentifier.checkCatalog(ident);

    List<CatalogUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toCatalogUpdateRequest)
            .collect(Collectors.toList());
    CatalogUpdatesRequest updatesRequest = new CatalogUpdatesRequest(reqs);
    updatesRequest.validate();

    CatalogResponse resp =
        restClient.put(
            String.format(API_METALAKES_CATALOGS_PATH, ident.namespace().level(0), ident.name()),
            updatesRequest,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(resp.getCatalog(), restClient);
  }

  /**
   * Drop the catalog with specified identifier.
   *
   * @param ident the identifier of the catalog.
   * @return true if the catalog is dropped successfully, false otherwise.
   */
  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    NameIdentifier.checkCatalog(ident);

    try {
      DropResponse resp =
          restClient.delete(
              String.format(API_METALAKES_CATALOGS_PATH, ident.namespace().level(0), ident.name()),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.catalogErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (Exception e) {
      LOG.warn("Failed to drop catalog {}", ident, e);
      return false;
    }
  }

  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GravitinoMetaLake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new GravitinoMetaLake(name, comment, properties, audit, restClient);
    }
  }
}
