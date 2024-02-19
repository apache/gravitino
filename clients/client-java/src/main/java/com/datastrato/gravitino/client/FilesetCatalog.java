/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.requests.FilesetCreateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdateRequest;
import com.datastrato.gravitino.dto.requests.FilesetUpdatesRequest;
import com.datastrato.gravitino.dto.responses.DropResponse;
import com.datastrato.gravitino.dto.responses.EntityListResponse;
import com.datastrato.gravitino.dto.responses.FilesetResponse;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Fileset catalog is a catalog implementation that supports fileset like metadata operations, for
 * example, schemas and filesets list, creation, update and deletion. A Fileset catalog is under the
 * metalake.
 */
public class FilesetCatalog extends BaseSchemaCatalog
    implements com.datastrato.gravitino.file.FilesetCatalog {

  FilesetCatalog(
      String name,
      Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(name, type, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public com.datastrato.gravitino.file.FilesetCatalog asFilesetCatalog()
      throws UnsupportedOperationException {
    return this;
  }

  /**
   * List the filesets in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace.
   * @return An array of fileset identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    Namespace.checkFileset(namespace);

    EntityListResponse resp =
        restClient.get(
            formatFilesetRequestPath(namespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.identifiers();
  }

  /**
   * Load fileset metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A fileset identifier.
   * @return The fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    NameIdentifier.checkFileset(ident);

    FilesetResponse resp =
        restClient.get(
            formatFilesetRequestPath(ident.namespace()) + "/" + ident.name(),
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.getFileset();
  }

  /**
   * Create a fileset metadata in the catalog.
   *
   * <p>If the type of the fileset object is "MANAGED", the underlying storageLocation can be null,
   * and Gravitino will manage the storage location based on the location of the schema.
   *
   * <p>If the type of the fileset object is "EXTERNAL", the underlying storageLocation must be set.
   *
   * @param ident A fileset identifier.
   * @param comment The comment of the fileset.
   * @param type The type of the fileset.
   * @param storageLocation The storage location of the fileset.
   * @param properties The properties of the fileset.
   * @return The created fileset metadata
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FilesetAlreadyExistsException If the fileset already exists.
   */
  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    NameIdentifier.checkFileset(ident);

    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(ident.name())
            .comment(comment)
            .type(type)
            .storageLocation(storageLocation)
            .properties(properties)
            .build();

    FilesetResponse resp =
        restClient.post(
            formatFilesetRequestPath(ident.namespace()),
            req,
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.getFileset();
  }

  /**
   * Update a fileset metadata in the catalog.
   *
   * @param ident A fileset identifier.
   * @param changes The changes to apply to the fileset.
   * @return The updated fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws IllegalArgumentException If the changes are invalid.
   */
  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    NameIdentifier.checkFileset(ident);

    List<FilesetUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toFilesetUpdateRequest)
            .collect(Collectors.toList());
    FilesetUpdatesRequest req = new FilesetUpdatesRequest(updates);

    FilesetResponse resp =
        restClient.put(
            formatFilesetRequestPath(ident.namespace()) + "/" + ident.name(),
            req,
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.getFileset();
  }

  /**
   * Drop a fileset from the catalog.
   *
   * <p>The underlying files will be deleted if this fileset type is managed, otherwise, only the
   * metadata will be dropped.
   *
   * @param ident A fileset identifier.
   * @return true If the fileset is dropped, false the fileset did not exist.
   */
  @Override
  public boolean dropFileset(NameIdentifier ident) {
    NameIdentifier.checkFileset(ident);

    DropResponse resp =
        restClient.delete(
            formatFilesetRequestPath(ident.namespace()) + "/" + ident.name(),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @VisibleForTesting
  static String formatFilesetRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(ns.level(2))
        .append("/filesets")
        .toString();
  }

  /**
   * Create a new builder for the fileset catalog.
   *
   * @return A new builder for the fileset catalog.
   */
  public static Builder builder() {
    return new Builder();
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;

    private Builder() {}

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public FilesetCatalog build() {
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new FilesetCatalog(name, type, provider, comment, properties, audit, restClient);
    }
  }
}
