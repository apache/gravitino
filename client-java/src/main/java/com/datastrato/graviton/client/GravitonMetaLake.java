/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.client;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.SupportsCatalogs;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.requests.CatalogCreateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdateRequest;
import com.datastrato.graviton.dto.requests.CatalogUpdatesRequest;
import com.datastrato.graviton.dto.responses.CatalogResponse;
import com.datastrato.graviton.dto.responses.DropResponse;
import com.datastrato.graviton.dto.responses.EntityListResponse;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitonMetaLake extends MetalakeDTO implements SupportsCatalogs {

  private static final Logger LOG = LoggerFactory.getLogger(GravitonMetaLake.class);

  private final RESTClient restClient;

  GravitonMetaLake(
      String name,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    validateCatalogNamespace(namespace);

    EntityListResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs", namespace.level(0)),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    validateCatalogIdentifier(ident);

    CatalogResponse resp =
        restClient.get(
            String.format("api/metalakes/%s/catalogs/%s", ident.namespace().level(0), ident.name()),
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(resp.getCatalog(), restClient);
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    validateCatalogIdentifier(ident);

    CatalogCreateRequest req = new CatalogCreateRequest(ident.name(), type, comment, properties);
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

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    validateCatalogIdentifier(ident);

    List<CatalogUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toCatalogUpdateRequest)
            .collect(Collectors.toList());
    CatalogUpdatesRequest updatesRequest = new CatalogUpdatesRequest(reqs);
    updatesRequest.validate();

    CatalogResponse resp =
        restClient.put(
            String.format("api/metalakes/%s/catalogs/%s", ident.namespace().level(0), ident.name()),
            updatesRequest,
            CatalogResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.catalogErrorHandler());
    resp.validate();

    return DTOConverters.toCatalog(resp.getCatalog(), restClient);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    validateCatalogIdentifier(ident);

    try {
      DropResponse resp =
          restClient.delete(
              String.format(
                  "api/metalakes/%s/catalogs/%s", ident.namespace().level(0), ident.name()),
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

  private static void validateCatalogNamespace(Namespace ns) {
    Preconditions.checkArgument(
        ns != null && ns.length() == 1, "namespace must not be null and have exactly one level");
  }

  private static void validateCatalogIdentifier(NameIdentifier ident) {
    validateCatalogNamespace(ident.namespace());
    Preconditions.checkArgument(
        StringUtils.isNotBlank(ident.name()), "name must not be null or empty");
  }

  static class Builder extends MetalakeDTO.Builder<Builder> {
    private RESTClient restClient;

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public GravitonMetaLake build() {
      Preconditions.checkNotNull(restClient, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be null or empty");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new GravitonMetaLake(name, comment, properties, audit, restClient);
    }
  }
}
