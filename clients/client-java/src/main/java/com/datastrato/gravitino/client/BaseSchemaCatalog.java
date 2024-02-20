/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.SchemaCreateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdateRequest;
import com.datastrato.gravitino.dto.requests.SchemaUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.SchemaResponse;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseSchemaCatalog is the base abstract class for all the catalog with schema. It provides the
 * common methods for managing schemas in a catalog. With {@link BaseSchemaCatalog}, users can list,
 * create, load, alter and drop a schema with specified identifier.
 */
abstract class BaseSchemaCatalog extends CatalogDTO implements SupportsSchemas {
  private static final Logger LOG = LoggerFactory.getLogger(BaseSchemaCatalog.class);

  /** The REST client to send the requests. */
  protected final RESTClient restClient;

  BaseSchemaCatalog(
      String name,
      Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO);
    this.restClient = restClient;
  }

  @Override
  public SupportsSchemas asSchemas() throws UnsupportedOperationException {
    return this;
  }

  /**
   * List all the schemas under the given catalog namespace.
   *
   * @param namespace The namespace of the catalog.
   * @return A list of {@link NameIdentifier} of the schemas under the given catalog namespace.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    Namespace.checkSchema(namespace);

    EntityListResponse resp =
        restClient.get(
            formatSchemaRequestPath(namespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Create a new schema with specified identifier, comment and metadata.
   *
   * @param ident The name identifier of the schema.
   * @param comment The comment of the schema.
   * @param properties The properties of the schema.
   * @return The created {@link Schema}.
   * @throws NoSuchCatalogException if the catalog with specified namespace does not exist.
   * @throws SchemaAlreadyExistsException if the schema with specified identifier already exists.
   */
  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    NameIdentifier.checkSchema(ident);

    SchemaCreateRequest req = new SchemaCreateRequest(ident.name(), comment, properties);
    req.validate();

    SchemaResponse resp =
        restClient.post(
            formatSchemaRequestPath(ident.namespace()),
            req,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
  }

  /**
   * Load the schema with specified identifier.
   *
   * @param ident The name identifier of the schema.
   * @return The {@link Schema} with specified identifier.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    NameIdentifier.checkSchema(ident);

    SchemaResponse resp =
        restClient.get(
            formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
  }

  /**
   * Alter the schema with specified identifier by applying the changes.
   *
   * @param ident The name identifier of the schema.
   * @param changes The metadata changes to apply.
   * @return The altered {@link Schema}.
   * @throws NoSuchSchemaException if the schema with specified identifier does not exist.
   */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    NameIdentifier.checkSchema(ident);

    List<SchemaUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toSchemaUpdateRequest)
            .collect(Collectors.toList());
    SchemaUpdatesRequest updatesRequest = new SchemaUpdatesRequest(reqs);
    updatesRequest.validate();

    SchemaResponse resp =
        restClient.put(
            formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
            updatesRequest,
            SchemaResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.schemaErrorHandler());
    resp.validate();

    return resp.getSchema();
  }

  /**
   * Drop the schema with specified identifier.
   *
   * @param ident The name identifier of the schema.
   * @param cascade Whether to drop all the tables under the schema.
   * @return true if the schema is dropped successfully, false otherwise.
   * @throws NonEmptySchemaException if the schema is not empty and cascade is false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    NameIdentifier.checkSchema(ident);

    try {
      DropResponse resp =
          restClient.delete(
              formatSchemaRequestPath(ident.namespace()) + "/" + ident.name(),
              Collections.singletonMap("cascade", String.valueOf(cascade)),
              DropResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.schemaErrorHandler());
      resp.validate();
      return resp.dropped();

    } catch (NonEmptySchemaException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to drop schema {}", ident, e);
      return false;
    }
  }

  @VisibleForTesting
  static String formatSchemaRequestPath(Namespace ns) {
    return new StringBuilder()
        .append("api/metalakes/")
        .append(ns.level(0))
        .append("/catalogs/")
        .append(ns.level(1))
        .append("/schemas")
        .toString();
  }
}
