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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.FilesetCreateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdatesRequest;
import org.apache.gravitino.dto.requests.GetFilesetContextRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.FilesetContextResponse;
import org.apache.gravitino.dto.responses.FilesetResponse;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.file.FilesetContext;
import org.apache.gravitino.file.FilesetDataOperationCtx;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Fileset catalog is a catalog implementation that supports fileset like metadata operations, for
 * example, schemas and filesets list, creation, update and deletion. A Fileset catalog is under the
 * metalake.
 */
class FilesetCatalog extends BaseSchemaCatalog implements org.apache.gravitino.file.FilesetCatalog {

  FilesetCatalog(
      Namespace namespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, name, type, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public org.apache.gravitino.file.FilesetCatalog asFilesetCatalog()
      throws UnsupportedOperationException {
    return this;
  }

  /**
   * List the filesets in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace. This namespace should have 1 level, which is the schema
   *     name;
   * @return An array of {@link NameIdentifier} of filesets under the given namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    checkFilesetNamespace(namespace);

    Namespace fullNamespace = getFilesetFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatFilesetRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load fileset metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A fileset identifier, which should be "schema.fileset" format.
   * @return The fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   */
  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    checkFilesetNameIdentifier(ident);

    Namespace fullNamespace = getFilesetFullNamespace(ident.namespace());
    FilesetResponse resp =
        restClient.get(
            formatFilesetRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return new GenericFileset(resp.getFileset(), restClient, fullNamespace);
  }

  /**
   * Create a fileset metadata in the catalog.
   *
   * <p>If the type of the fileset object is "MANAGED", the underlying storageLocation can be null,
   * and Gravitino will manage the storage location based on the location of the schema.
   *
   * <p>If the type of the fileset object is "EXTERNAL", the underlying storageLocation must be set.
   *
   * @param ident A fileset identifier, which should be "schema.fileset" format.
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
    checkFilesetNameIdentifier(ident);

    Namespace fullNamespace = getFilesetFullNamespace(ident.namespace());
    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name(RESTUtils.encodeString(ident.name()))
            .comment(comment)
            .type(type)
            .storageLocation(storageLocation)
            .properties(properties)
            .build();

    FilesetResponse resp =
        restClient.post(
            formatFilesetRequestPath(fullNamespace),
            req,
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return new GenericFileset(resp.getFileset(), restClient, fullNamespace);
  }

  /**
   * Update a fileset metadata in the catalog.
   *
   * @param ident A fileset identifier, which should be "schema.fileset" format.
   * @param changes The changes to apply to the fileset.
   * @return The updated fileset metadata.
   * @throws NoSuchFilesetException If the fileset does not exist.
   * @throws IllegalArgumentException If the changes are invalid.
   */
  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    checkFilesetNameIdentifier(ident);

    Namespace fullNamespace = getFilesetFullNamespace(ident.namespace());
    List<FilesetUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toFilesetUpdateRequest)
            .collect(Collectors.toList());
    FilesetUpdatesRequest req = new FilesetUpdatesRequest(updates);
    req.validate();

    FilesetResponse resp =
        restClient.put(
            formatFilesetRequestPath(fullNamespace) + "/" + ident.name(),
            req,
            FilesetResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return new GenericFileset(resp.getFileset(), restClient, fullNamespace);
  }

  /**
   * Drop a fileset from the catalog.
   *
   * <p>The underlying files will be deleted if this fileset type is managed, otherwise, only the
   * metadata will be dropped.
   *
   * @param ident A fileset identifier, which should be "schema.fileset" format.
   * @return true If the fileset is dropped, false the fileset did not exist.
   */
  @Override
  public boolean dropFileset(NameIdentifier ident) {
    checkFilesetNameIdentifier(ident);

    Namespace fullNamespace = getFilesetFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatFilesetRequestPath(fullNamespace) + "/" + ident.name(),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  /**
   * Get a fileset context from the catalog.
   *
   * @param ident A fileset identifier.
   * @param ctx The data operation context.
   * @return The fileset context.
   */
  @Override
  public FilesetContext getFilesetContext(NameIdentifier ident, FilesetDataOperationCtx ctx)
      throws NoSuchFilesetException {
    checkFilesetNameIdentifier(ident);
    Namespace fullNamespace = getFilesetFullNamespace(ident.namespace());
    GetFilesetContextRequest req =
        GetFilesetContextRequest.builder()
            .subPath(ctx.subPath())
            .operation(ctx.operation())
            .clientType(ctx.clientType())
            .build();

    FilesetContextResponse resp =
        restClient.post(
            formatFilesetRequestPath(fullNamespace) + "/" + ident.name() + "/" + "context",
            req,
            FilesetContextResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.filesetErrorHandler());
    resp.validate();

    return resp.getContext();
  }

  @VisibleForTesting
  static String formatFilesetRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/filesets")
        .toString();
  }

  /**
   * Check whether the namespace of a fileset is valid.
   *
   * @param namespace The namespace to check.
   */
  static void checkFilesetNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Fileset namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a fileset is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.fileset" format.
   */
  static void checkFilesetNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkFilesetNamespace(ident.namespace());
  }

  /**
   * Get the full namespace of the fileset with the given fileset's short namespace (schema name).
   *
   * @param filesetNamespace The fileset's short namespace, which is the schema name.
   * @return full namespace of the fileset, which is "metalake.catalog.schema" format.
   */
  private Namespace getFilesetFullNamespace(Namespace filesetNamespace) {
    return Namespace.of(this.catalogNamespace().level(0), this.name(), filesetNamespace.level(0));
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
    /** The namespace of the catalog */
    private Namespace namespace;

    private Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public FilesetCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new FilesetCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
